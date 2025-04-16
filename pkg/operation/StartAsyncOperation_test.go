package operation

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	"time"

	oc "github.com/Azure/OperationContainer/api/v1"
	ocMock "github.com/Azure/OperationContainer/api/v1/mock"
	opbus "github.com/Azure/aks-async/operationsbus"
	"github.com/Azure/aks-async/servicebus"
	"github.com/DATA-DOG/go-sqlmock"

	asyncMocks "github.com/Azure/aks-async/mocks"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	gomock "go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestServer(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "StartAsyncOperation Suite")
}

const (
	entityTableUpdateQuery = `
      MERGE INTO %s AS target
      USING \(SELECT @p1 AS entity_id, @p2 AS entity_type, @p3 AS last_operation_id, @p4 AS operation_name, @p5 AS operation_status\) AS source
      ON target.entity_id = source.entity_id and target.entity_type = source.entity_type
      WHEN MATCHED AND \(target.operation_status = 'COMPLETED' OR target.operation_status = 'FAILED' OR target.operation_status = 'CANCELLED' OR target.operation_status = 'UNKNOWN'\) THEN
        UPDATE SET
          target.last_operation_id = source.last_operation_id,
          target.operation_name = source.operation_name,
          target.operation_status = source.operation_status
      WHEN NOT MATCHED THEN
        INSERT \(entity_id, entity_type, last_operation_id, operation_name, operation_status\)
        VALUES \(source.entity_id, source.entity_type, source.last_operation_id, source.operation_name, source.operation_status\);
     `
)

// Match satisfies sqlmock.Argument interface.
// Required for checking that the operationId matches a string format.
type AnyString struct{}

func (a AnyString) Match(v driver.Value) bool {
	_, ok := v.(string)
	return ok
}

var _ = Describe("Mock Testing", func() {
	var (
		ctx                      context.Context
		ctrl                     *gomock.Controller
		mockSender               *asyncMocks.MockSenderInterface
		db                       *sql.DB
		mockDb                   sqlmock.Sqlmock
		entityTableName          string
		query                    string
		operationContainerClient *ocMock.MockOperationContainerClient
		serviceBusSender         servicebus.SenderInterface

		operationName       string
		apiVersion          string
		body                []byte
		httpMethod          string
		retryCount          int
		entityId            string
		entityType          string
		expirationTimestamp *timestamppb.Timestamp
	)

	BeforeEach(func() {
		ctx = context.Background()
		ctrl = gomock.NewController(GinkgoT())
		mockSender = asyncMocks.NewMockSenderInterface(ctrl)
		serviceBusSender = mockSender
		db, mockDb, _ = sqlmock.New()
		operationContainerClient = ocMock.NewMockOperationContainerClient(ctrl)

		entityTableName = "hcp"
		operationName = "test"
		apiVersion = "test"
		body = nil
		httpMethod = "test"
		retryCount = 0
		entityId = "test"
		entityType = "test"
		expirationTimestamp = timestamppb.New(time.Now().Add(1 * time.Hour))

		query = fmt.Sprintf(entityTableUpdateQuery, entityTableName)
	})

	AfterEach(func() {
		err := mockDb.ExpectationsWereMet()
		Expect(err).To(BeNil())
		db.Close()
		ctrl.Finish()
	})

	Context("async operations", func() {
		It("should return operationId and insert new operation into database", func() {
			initialOperationStatus := oc.Status_PENDING.String()

			// Need to use AnyString{} since we only care that it's a string, not really the values of it.
			mockDb.ExpectExec(query).WithArgs(entityId, entityType, AnyString{}, operationName, initialOperationStatus).WillReturnResult(sqlmock.NewResult(1, 1))
			operationContainerClient.EXPECT().CreateOperationStatus(gomock.Any(), gomock.Any()).Return(nil, nil)
			mockSender.EXPECT().SendMessage(gomock.Any(), gomock.Any()).Return(nil).Times(1)

			out, err := StartLongRunningOperation(ctx, operationName, apiVersion, body, httpMethod, retryCount, entityId, entityType, expirationTimestamp, entityTableName, serviceBusSender, operationContainerClient, db)

			Expect(err).To(BeNil())
			Expect(out).NotTo(BeNil())
		})

		It("should fail on sender failure", func() {
			errorMessage := "ServiceBus Sender error"
			err := errors.New(errorMessage)
			mockSender.EXPECT().SendMessage(gomock.Any(), gomock.Any()).Return(err).Times(1)

			out, err := StartLongRunningOperation(ctx, operationName, apiVersion, body, httpMethod, retryCount, entityId, entityType, expirationTimestamp, entityTableName, serviceBusSender, operationContainerClient, db)

			Expect(err).ToNot(BeNil())
			Expect(err.Error()).To(ContainSubstring(errorMessage))
			Expect(out).To(Equal(""))
		})

		It("should call OperationContainer to update operation database on entity database query failure", func() {
			initialOperationStatus := oc.Status_PENDING.String()

			mockSender.EXPECT().SendMessage(gomock.Any(), gomock.Any()).Return(nil).Times(1)
			operationContainerClient.EXPECT().CreateOperationStatus(gomock.Any(), gomock.Any()).Return(nil, nil)

			// Need to use AnyString{} since we only care that it's a string, not really the values of it.
			errorMessage := "Database failure error."
			dbErr := errors.New(errorMessage)
			mockDb.ExpectExec(query).WithArgs(entityId, entityType, AnyString{}, operationName, initialOperationStatus).WillReturnError(dbErr)

			operationContainerClient.EXPECT().UpdateOperationStatus(gomock.Any(), gomock.Any()).Return(nil, nil)
			out, err := StartLongRunningOperation(ctx, operationName, apiVersion, body, httpMethod, retryCount, entityId, entityType, expirationTimestamp, entityTableName, serviceBusSender, operationContainerClient, db)

			Expect(err).ToNot(BeNil())
			Expect(err.Error()).To(ContainSubstring(errorMessage))
			Expect(out).To(Equal(""))
		})

		It("should throw an error if update operations database fails after entity database update failure", func() {
			mockSender.EXPECT().SendMessage(gomock.Any(), gomock.Any()).Return(nil).Times(1)
			operationContainerClient.EXPECT().CreateOperationStatus(gomock.Any(), gomock.Any()).Return(nil, nil)

			initialOperationStatus := oc.Status_PENDING.String()

			dbErrorMessage := "Database failure error."
			dbErr := errors.New(dbErrorMessage)
			// Need to use AnyString{} since we only care that it's a string, not really the values of it.
			mockDb.ExpectExec(query).WithArgs(entityId, entityType, AnyString{}, operationName, initialOperationStatus).WillReturnError(dbErr)

			ocErrorMessage := "Something went wrong with OperationContainer!"
			ocErr := errors.New(ocErrorMessage)
			operationContainerClient.EXPECT().UpdateOperationStatus(gomock.Any(), gomock.Any()).Return(nil, ocErr)

			out, err := StartLongRunningOperation(ctx, operationName, apiVersion, body, httpMethod, retryCount, entityId, entityType, expirationTimestamp, entityTableName, serviceBusSender, operationContainerClient, db)

			Expect(err).NotTo(BeNil())
			Expect(err.Error()).To(ContainSubstring(dbErrorMessage))
			Expect(err.Error()).To(ContainSubstring(ocErrorMessage))
			Expect(out).To(Equal(""))
		})
	})
})

var _ = Describe("Fakes Testing", func() {
	var (
		ctx  context.Context
		ctrl *gomock.Controller

		db                       *sql.DB
		mockDb                   sqlmock.Sqlmock
		operationContainerClient *ocMock.MockOperationContainerClient
		serviceBusClient         servicebus.ServiceBusClientInterface
		serviceBusSender         servicebus.SenderInterface
		entityTableName          string
		query                    string

		operationName       string
		apiVersion          string
		body                []byte
		httpMethod          string
		retryCount          int
		entityId            string
		entityType          string
		expirationTimestamp *timestamppb.Timestamp
	)

	BeforeEach(func() {
		ctx = context.Background()
		ctrl = gomock.NewController(GinkgoT())

		serviceBusClient = servicebus.NewFakeServiceBusClient()
		serviceBusSender, _ = serviceBusClient.NewServiceBusSender(nil, "", nil)
		db, mockDb, _ = sqlmock.New()
		operationContainerClient = ocMock.NewMockOperationContainerClient(ctrl)

		entityTableName = "hcp"
		operationName = "test"
		apiVersion = "test"
		body = nil
		httpMethod = "test"
		retryCount = 0
		entityId = "test"
		entityType = "test"
		expirationTimestamp = timestamppb.New(time.Now().Add(1 * time.Hour))

		query = fmt.Sprintf(entityTableUpdateQuery, entityTableName)
	})

	AfterEach(func() {
		err := mockDb.ExpectationsWereMet()
		Expect(err).To(BeNil())
	})

	Context("Message should exist in the service bus", func() {
		It("should send the message successfully", func() {
			initialOperationStatus := oc.Status_PENDING.String()

			// Can mostly ignore this since we tested it above. We care more about the service bus mock in this test.
			// Need to add it because otherwise the server call will complain froma null pointer to try and access the db
			// if it doesn't exist in the test.
			mockDb.ExpectExec(query).WithArgs(entityId, entityType, AnyString{}, operationName, initialOperationStatus).WillReturnResult(sqlmock.NewResult(1, 1))

			operationContainerClient.EXPECT().CreateOperationStatus(gomock.Any(), gomock.Any()).Return(nil, nil)

			out, err := StartLongRunningOperation(ctx, operationName, apiVersion, body, httpMethod, retryCount, entityId, entityType, expirationTimestamp, entityTableName, serviceBusSender, operationContainerClient, db)
			Expect(err).ToNot(HaveOccurred())
			Expect(out).ToNot(Equal(""))

			sbReceiver, _ := serviceBusClient.NewServiceBusReceiver(nil, "", nil)
			msg, err := sbReceiver.ReceiveMessage(nil)
			Expect(err).ToNot(HaveOccurred())
			Expect(msg).NotTo(BeNil())

			opRequestExpected := opbus.NewOperationRequest(operationName, apiVersion, "", entityId, entityType, retryCount, expirationTimestamp, body, httpMethod, nil)

			var opRequestReceived opbus.OperationRequest
			err = json.Unmarshal(msg, &opRequestReceived)
			Expect(err).ToNot(HaveOccurred())

			Expect(opRequestReceived.OperationName).To(Equal(opRequestExpected.OperationName))
			Expect(opRequestReceived.OperationId).NotTo(Equal("")) // Can't ensure operationId is exactly equal since it's generated
			Expect(opRequestReceived.RetryCount).To(Equal(opRequestExpected.RetryCount))
			Expect(opRequestReceived.EntityType).To(Equal(opRequestExpected.EntityType))
			Expect(opRequestReceived.EntityId).To(Equal(opRequestExpected.EntityId))
			Expect(opRequestReceived.ExpirationTimestamp).To(Equal(opRequestExpected.ExpirationTimestamp))
			Expect(opRequestReceived.APIVersion).To(Equal(opRequestExpected.APIVersion))
		})
	})
})
