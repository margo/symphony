package margo

import (
    "context"
    "encoding/json"
    "errors"
    "testing"

    "github.com/eclipse-symphony/symphony/coa/pkg/apis/v1alpha2/contexts"
    "github.com/eclipse-symphony/symphony/coa/pkg/apis/v1alpha2/managers"
    "github.com/eclipse-symphony/symphony/coa/pkg/apis/v1alpha2/providers"
    "github.com/eclipse-symphony/symphony/coa/pkg/apis/v1alpha2/providers/states"
    margoAPIModels "github.com/margo/dev-repo/non-standard/generatedCode/models"
    "github.com/stretchr/testify/assert"
    "github.com/stretchr/testify/mock"
)

// Mock StateProvider
type MockStateProvider struct {
    mock.Mock
}

func (m *MockStateProvider) Init(config providers.IProviderConfig) error {
    args := m.Called(config)
    return args.Error(0)
}

func (m *MockStateProvider) SetContext(ctx *contexts.ManagerContext) {
    m.Called(ctx)
}

func (m *MockStateProvider) InitWithMap(properties map[string]string) error {
    args := m.Called(properties)
    return args.Error(0)
}

func (m *MockStateProvider) Get(ctx context.Context, request states.GetRequest) (states.StateEntry, error) {
    args := m.Called(ctx, request)
    return args.Get(0).(states.StateEntry), args.Error(1)
}

func (m *MockStateProvider) Upsert(ctx context.Context, request states.UpsertRequest) (string, error) {
    args := m.Called(ctx, request)
    return args.String(0), args.Error(1)
}

func (m *MockStateProvider) Delete(ctx context.Context, request states.DeleteRequest) error {
    args := m.Called(ctx, request)
    return args.Error(0)
}

func (m *MockStateProvider) List(ctx context.Context, request states.ListRequest) ([]states.StateEntry, string, error) {
    args := m.Called(ctx, request)
    return args.Get(0).([]states.StateEntry), args.String(1), args.Error(2)
}

func TestMargoManager_Init(t *testing.T) {
    tests := []struct {
        name          string
        setupMocks    func(*MockStateProvider)
        config        managers.ManagerConfig
        expectError   bool
        errorContains string
    }{
        {
            name: "successful initialization",
            setupMocks: func(mockProvider *MockStateProvider) {
                mockProvider.On("SetContext", mock.AnythingOfType("*contexts.ManagerContext"))
            },
            config: managers.ManagerConfig{
                Properties: map[string]string{
                    "providers.persistentstate": "mock-provider",
                },
            },
            expectError: false,
        },
        {
            name: "missing state provider",
            setupMocks: func(mockProvider *MockStateProvider) {
                // No setup - provider won't be found
            },
            config:        managers.ManagerConfig{},
            expectError:   true,
            errorContains: "state provider",
        },
    }

    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            mockStateProvider := &MockStateProvider{}
            tt.setupMocks(mockStateProvider)

            manager := &MargoManager{}
            vendorCtx := &contexts.VendorContext{}
            
            providers := map[string]providers.IProvider{
                "mock-provider": mockStateProvider,
            }

            err := manager.Init(vendorCtx, tt.config, providers)

            if tt.expectError {
                assert.Error(t, err)
                if tt.errorContains != "" {
                    assert.Contains(t, err.Error(), tt.errorContains)
                }
            } else {
                assert.NoError(t, err)
                assert.NotNil(t, manager.StateProvider)
            }

            mockStateProvider.AssertExpectations(t)
        })
    }
}

func TestMargoManager_OnboardAppPkg(t *testing.T) {
    tests := []struct {
        name          string
        spec          margoAPIModels.AppPkgOnboardingReq
        setupMocks    func(*MockStateProvider)
        expectError   bool
        errorContains string
    }{
        {
            name: "successful onboarding",
            spec: margoAPIModels.AppPkgOnboardingReq{
                Name:       "test-app",
                SourceType: margoAPIModels.AppPkgOnboardingReqSourceTypeGITREPO,
            },
            setupMocks: func(mockProvider *MockStateProvider) {
                mockProvider.On("Upsert", mock.Anything, mock.AnythingOfType("states.UpsertRequest")).
                    Return("success", nil)
            },
            expectError: false,
        },
        {
            name: "empty package name",
            spec: margoAPIModels.AppPkgOnboardingReq{
                Name:       "",
                SourceType: margoAPIModels.AppPkgOnboardingReqSourceTypeGITREPO,
            },
            setupMocks: func(mockProvider *MockStateProvider) {
                // No database calls expected
            },
            expectError:   true,
            errorContains: "package name is required",
        },
        {
            name: "database error",
            spec: margoAPIModels.AppPkgOnboardingReq{
                Name:       "test-app",
                SourceType: margoAPIModels.AppPkgOnboardingReqSourceTypeGITREPO,
            },
            setupMocks: func(mockProvider *MockStateProvider) {
                mockProvider.On("Upsert", mock.Anything, mock.AnythingOfType("states.UpsertRequest")).
                    Return("", errors.New("database error"))
            },
            expectError:   true,
            errorContains: "failed to store app pkg in database",
        },
    }

    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            mockStateProvider := &MockStateProvider{}
            tt.setupMocks(mockStateProvider)

            manager := &MargoManager{
                StateProvider: mockStateProvider,
            }

            ctx := context.Background()
            resp, err := manager.OnboardAppPkg(ctx, tt.spec)

            if tt.expectError {
                assert.Error(t, err)
                assert.Nil(t, resp)
                if tt.errorContains != "" {
                    assert.Contains(t, err.Error(), tt.errorContains)
                }
            } else {
                assert.NoError(t, err)
                assert.NotNil(t, resp)
                assert.NotEmpty(t, resp.Id)
                assert.Equal(t, tt.spec.Name, resp.Name)
                assert.Equal(t, margoAPIModels.Onboard, resp.Operation)
                assert.Equal(t, margoAPIModels.Pending, resp.OperationState)
            }

            mockStateProvider.AssertExpectations(t)
        })
    }
}

func TestMargoManager_ListAppPkgs(t *testing.T) {
    tests := []struct {
        name          string
        setupMocks    func(*MockStateProvider)
        expectError   bool
        expectedCount int
    }{
        {
            name: "successful listing with packages",
            setupMocks: func(mockProvider *MockStateProvider) {
                pkg1 := margoAPIModels.AppPkg{
                    Id:   "pkg-1",
                    Name: "app-one",
                }
                pkg2 := margoAPIModels.AppPkg{
                    Id:   "pkg-2",
                    Name: "app-two",
                }

                pkg1Bytes, _ := json.Marshal(pkg1)
                pkg2Bytes, _ := json.Marshal(pkg2)

                entries := []states.StateEntry{
                    {ID: "pkg-1", Body: pkg1Bytes},
                    {ID: "pkg-2", Body: pkg2Bytes},
                }

                mockProvider.On("List", mock.Anything, mock.AnythingOfType("states.ListRequest")).
                    Return(entries, "", nil)
            },
            expectError:   false,
            expectedCount: 2,
        },
        {
            name: "empty list",
            setupMocks: func(mockProvider *MockStateProvider) {
                mockProvider.On("List", mock.Anything, mock.AnythingOfType("states.ListRequest")).
                    Return([]states.StateEntry{}, "", nil)
            },
            expectError:   false,
            expectedCount: 0,
        },
        {
            name: "database error",
            setupMocks: func(mockProvider *MockStateProvider) {
                mockProvider.On("List", mock.Anything, mock.AnythingOfType("states.ListRequest")).
                    Return([]states.StateEntry{}, "", errors.New("database error"))
            },
            expectError: true,
        },
    }

    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            mockStateProvider := &MockStateProvider{}
            tt.setupMocks(mockStateProvider)

            manager := &MargoManager{
                StateProvider: mockStateProvider,
            }

            ctx := context.Background()
            resp, err := manager.ListAppPkgs(ctx)

            if tt.expectError {
                assert.Error(t, err)
                assert.Nil(t, resp)
            } else {
                assert.NoError(t, err)
                assert.NotNil(t, resp)
                assert.Len(t, resp, tt.expectedCount)
            }

            mockStateProvider.AssertExpectations(t)
        })
    }
}

func TestMargoManager_GetAppPkg(t *testing.T) {
    tests := []struct {
        name          string
        pkgId         string
        setupMocks    func(*MockStateProvider)
        expectError   bool
        errorContains string
    }{
        {
            name:  "successful get",
            pkgId: "test-pkg-id",
            setupMocks: func(mockProvider *MockStateProvider) {
                pkg := margoAPIModels.AppPkg{
                    Id:   "test-pkg-id",
                    Name: "test-app",
                }

                pkgBytes, _ := json.Marshal(pkg)
                entry := states.StateEntry{
                    ID:   "test-pkg-id",
                    Body: pkgBytes,
                }

                mockProvider.On("Get", mock.Anything, mock.AnythingOfType("states.GetRequest")).
                    Return(entry, nil)
            },
            expectError: false,
        },
        {
            name:  "package not found",
            pkgId: "non-existent-id",
            setupMocks: func(mockProvider *MockStateProvider) {
                mockProvider.On("Get", mock.Anything, mock.AnythingOfType("states.GetRequest")).
                    Return(states.StateEntry{}, errors.New("not found"))
            },
            expectError:   true,
            errorContains: "not found",
        },
    }

    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            mockStateProvider := &MockStateProvider{}
            tt.setupMocks(mockStateProvider)

            manager := &MargoManager{
                StateProvider: mockStateProvider,
            }

            ctx := context.Background()
            resp, err := manager.GetAppPkg(ctx, tt.pkgId)

            if tt.expectError {
                assert.Error(t, err)
                assert.Nil(t, resp)
                if tt.errorContains != "" {
                    assert.Contains(t, err.Error(), tt.errorContains)
                }
            } else {
                assert.NoError(t, err)
                assert.NotNil(t, resp)
                assert.Equal(t, tt.pkgId, resp.Id)
            }

            mockStateProvider.AssertExpectations(t)
        })
    }
}

func TestMargoManager_DeleteAppPkg(t *testing.T) {
    tests := []struct {
        name          string
        pkgId         string
        setupMocks    func(*MockStateProvider)
        expectError   bool
        errorContains string
    }{
        {
            name:  "successful deletion initiation",
            pkgId: "test-pkg-id",
            setupMocks: func(mockProvider *MockStateProvider) {
                pkg := margoAPIModels.AppPkg{
                    Id:             "test-pkg-id",
                    Name:           "test-app",
                    Operation:      margoAPIModels.Onboard,
                    OperationState: margoAPIModels.Completed,
                }

                pkgBytes, _ := json.Marshal(pkg)
                entry := states.StateEntry{
                    ID:   "test-pkg-id",
                    Body: pkgBytes,
                }

                // First call to get the package
                mockProvider.On("Get", mock.Anything, mock.AnythingOfType("states.GetRequest")).
                    Return(entry, nil)

                // Second call to update the package state
                mockProvider.On("Upsert", mock.Anything, mock.AnythingOfType("states.UpsertRequest")).
                    Return("success", nil)
            },
            expectError: false,
        },
        {
            name:  "empty package ID",
            pkgId: "",
            setupMocks: func(mockProvider *MockStateProvider) {
                // No database calls expected
            },
            expectError:   true,
            errorContains: "package ID is required",
        },
        {
            name:  "package not found",
            pkgId: "non-existent-id",
            setupMocks: func(mockProvider *MockStateProvider) {
                mockProvider.On("Get", mock.Anything, mock.AnythingOfType("states.GetRequest")).
                    Return(states.StateEntry{}, errors.New("not found"))
            },
            expectError:   true,
            errorContains: "failed to check the latest state",
        },
    }

    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            mockStateProvider := &MockStateProvider{}
            tt.setupMocks(mockStateProvider)

            manager := &MargoManager{
                StateProvider: mockStateProvider,
            }

            ctx := context.Background()
            resp, err := manager.DeleteAppPkg(ctx, tt.pkgId)

            if tt.expectError {
                assert.Error(t, err)
                assert.Nil(t, resp)
                if tt.errorContains != "" {
                    assert.Contains(t, err.Error(), tt.errorContains)
                }
            } else {
                assert.NoError(t, err)
                assert.NotNil(t, resp)
                assert.Equal(t, tt.pkgId, resp.Id)
                assert.Equal(t, margoAPIModels.Deboard, resp.Operation)
                assert.Equal(t, margoAPIModels.Pending, resp.OperationState)
                assert.NotNil(t, resp.Message)
            }

            mockStateProvider.AssertExpectations(t)
        })
    }
}

func TestMargoManager_EdgeCases(t *testing.T) {
    t.Run("onboard with nil state provider", func(t *testing.T) {
        manager := &MargoManager{
            StateProvider: nil,
        }

        spec := margoAPIModels.AppPkgOnboardingReq{
            Name:       "test-app",
            SourceType: margoAPIModels.AppPkgOnboardingReqSourceTypeGITREPO,
        }

        ctx := context.Background()
        resp, err := manager.OnboardAppPkg(ctx, spec)

        assert.Error(t, err)
        assert.Nil(t, resp)
    })

    t.Run("list with nil state provider", func(t *testing.T) {
        manager := &MargoManager{
            StateProvider: nil,
        }

        ctx := context.Background()
        resp, err := manager.ListAppPkgs(ctx)

        assert.Error(t, err)
        assert.Nil(t, resp)
    })
}
