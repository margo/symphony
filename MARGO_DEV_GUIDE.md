# Developer Guide — Margo Integration on Top of Symphony

This repository is a fork of [Symphony](https://github.com/eclipse-symphony/symphony) and has been adapted to support an end-to-end Proof of Concept (PoC) for the Margo ecosystem.

The repository demonstrates how the [Margo Specification](https://github.com/margo/specification) can be implemented on top of Symphony while also introducing additional APIs and workflows needed for real-world orchestration and user interaction.

This document explains:

* the architectural model,
* how Margo extends Symphony,
* how APIs are added or modified,
* how the CLI interacts with the system,
* where generated code comes from,
* and the expected contributor workflow.

---

**NOTE:** All development tasks will be branched out from `development` only. The branches, `main` and `development` are protected and hence you can't make any contributions directly let alone a PR is allowed. We follow the same branching and git philosophy as mentioned [here in sandbox repo](https://github.com/margo/sandbox/blob/main/CONTRIBUTING.md) . 

# Required Background Reading

Before contributing, review the following repositories and documents.

| Resource                                                                                                                   | Purpose                                                     |
| -------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------- |
| [Margo Specification](https://github.com/margo/specification)                                       | Defines the official Margo APIs and behavior                |
| [Sandbox Repository](https://github.com/margo/sandbox)                                              | Shared specs, generated models, SDKs, integration workflows |
| [Symphony Internals Discussion](https://github.com/margo/sandbox/issues/12#issuecomment-2969577828) | Explains Symphony internals and extension limitations       |
| Extension Pattern                                                                                   | Explains how Margo integrates into Symphony                 |

---

# Recommended Reading Order

```text
1. Extension Pattern
2. Sandbox Repository Structure
3. Symphony Internals (optional initially)
```

The Symphony internals document is useful for advanced understanding, but contributors can begin development without fully understanding Symphony internals.

The Extension Pattern section alone is sufficient to start implementing APIs and workflows.

---

# High-Level Architecture

Symphony organizes its internal architecture into three primary components.

```text
+-------------------+
|     Manager (M)   |
|-------------------|
| Business Logic    |
+-------------------+

          ↑

+-------------------+
|     Vendor (V)    |
|-------------------|
| API Layer         |
| Extension Point   |
+-------------------+

          ↑

+-------------------+
|    Provider (P)   |
|-------------------|
| External Systems  |
| Plugins/Adapters  |
+-------------------+
```

This structure is referred to as the:

# HB-MVP Pattern

---

# Component Responsibilities

| Component    | Responsibility                      |
| ------------ | ----------------------------------- |
| Manager (M)  | Business logic and orchestration    |
| Vendor (V)   | API controllers and extension layer |
| Provider (P) | Communication with external systems |

Examples of Providers include:

* databases,
* cloud services,
* external workflow managers,
* storage systems,
* messaging systems,
* infrastructure integrations.

---

# How Margo Extends Symphony

Margo primarily extends the **Vendor layer**.

```text
                +----------------------+
                |     User / CLI       |
                +----------------------+
                           |
                           v
                +----------------------+
                |   Margo Vendor APIs  |
                +----------------------+
                           |
                           v
                +----------------------+
                |    Manager Logic     |
                +----------------------+
                           |
                           v
                +----------------------+
                |  Providers/Services  |
                +----------------------+
```

The Vendor layer is used to expose both:

1. Standard Margo APIs
2. Non-standard extension APIs

---

# Standard vs Non-Standard APIs

## Standard APIs

These APIs are defined by the official Margo specification.

---

## Non-Standard APIs

These APIs are outside the official Margo specification.

They exist solely to enable complete end-to-end workflows and user interaction flows that were not achievable using the original Symphony APIs alone.

---

# Why Were Non-Standard APIs Needed?

Two major reasons led to their introduction.

## 1. Missing User Interaction Workflows

The existing Symphony APIs were not complete reusable, and required some extension or rewriting in some places:

* triggering deployments,
* driving workflow execution,
* user-driven orchestration,
* interactive execution flows.

---

## 2. End-to-End Workflow Completion

Several orchestration scenarios required additional APIs that were not formally part of the Margo specification.

Instead of embedding these workflows directly into Symphony internals, extension APIs were introduced using Margo-oriented conventions.

---

# Why Keep Non-Standard APIs Separate?

The non-standard APIs are intentionally because they serve as:

* experimentation layers,
* reusable SDK contracts for other WFM ecosystems.

This allows:

* other workflow frameworks to borrow ideas,
* API reuse if a vendor likes it,

---

# Important Architectural Decision

Initially, the implementation attempted to convert Margo objects into Symphony’s internal state model in order to reuse Symphony’s state management system.

However, this approach was eventually abandoned because:

* Symphony’s internal target agent assumptions became difficult to preserve,
* large rewrites would have been required,
* integration complexity became too high.

As a result:

* Margo logic was intentionally kept separate,
* conversion into native Symphony internal objects is currently not emphasized,
* Margo maintains its own orchestration flow on top of Symphony infrastructure.

---

# Repository Outputs

This repository currently produces two primary executables.

---

# 1. Symphony API Server

The API server contains:

* Vendors
* Managers
* Providers

```text
+----------------------------------+
|      Symphony API Server         |
|----------------------------------|
| Vendors                          |
| Managers                         |
| Providers                        |
+----------------------------------+
```

---

# 2. Maestro CLI

The Maestro CLI is used to interact with the Symphony API server.

```text
+----------------+
|  Maestro CLI   |
+----------------+
         |
         v
+----------------------+
| Generated Client SDK |
| (from Sandbox)       |
+----------------------+
         |
         v
+----------------------+
| Symphony API Server  |
+----------------------+
```

---

NOTE: A web UI exists in the repository, but it had some issues in displaying Solution and Target objects and hence was dumped.

---

# Relationship Between Symphony and Sandbox

The [Sandbox Repository](https://github.com/margo/sandbox) plays a central role in the architecture.

It contains:

* API specifications,
* generated models,
* generated client SDKs,
* reusable contracts,
* integration workflows.

---

# Generated vs Manual Code

## Generated

The following are generated from Sandbox specifications:

* request/response models,
* api clients

These generated artifacts are committed directly into the Sandbox repository.

---

## Manual

The Symphony server implementation is written manually, because the server layer can't use the auto-generated oapi-codegen codebase.

---

# Overall Architecture Flow

```text
                +----------------------+
                |   Sandbox Specs      |
                |  (Standard + Custom) |
                +----------------------+
                           |
                           v
                +----------------------+
                | Code Generation      |
                | Models + Client SDK  |
                +----------------------+
                           |
          +----------------+----------------+
          |                                 |
          v                                 v
+----------------------+      +----------------------+
|    Maestro CLI       |      | Symphony API Server |
| Uses Generated SDK   |      | Manual Controllers  |
+----------------------+      +----------------------+
```

---

# Extending the Symphony API Server

# Scenario 1 — Adding a New API

---

## Step 1 — Modify the Sandbox Specification

Navigate to the non-standard spec area inside the Sandbox repository.

```text
sandbox/
└── non-standard/
    └── <spec-files>
```

Modify or add the API definition as needed.

---

## Step 2 — Generate Models and Client SDK

Run the generation shell script provided in the Sandbox repository.

This generates:

* request/response models,
* client SDKs,
* API bindings.

---

## Step 3 — Implement the Vendor Controller

Inside Symphony:

```text
api/pkg/apis/v1alpha1/vendors/margo/
```

Add the Vendor/controller implementation for the new API.

The Vendor layer is responsible for:

* receiving API requests,
* request adaptation,
* validation,
* delegating work to Managers.

---

## Step 4 — Implement Business Logic

Inside:

```text
managers/margo/
```

Implement the actual orchestration/business logic.

The Vendor layer should remain thin and delegate operational logic to Managers.

---

# Final Request Flow

```text
Client Request
      |
      v
+----------------+
| Vendor API     |
+----------------+
      |
      v
+----------------+
| Manager Logic  |
+----------------+
      |
      v
+----------------+
| Providers      |
+----------------+
```

---

# Scenario 2 — Modifying Existing Business Logic

If the API already exists and only runtime behavior must change:

Navigate to:

```text
managers/margo/
```

Locate the relevant manager implementation and modify the business logic directly.

No spec regeneration is required unless the API contract changes.

---

# Inter-Manager Communication

Managers may communicate using an internal message queue abstraction.

The queue backend is configurable through:

```text
api/symphony-api-margo.json
```

The queue abstraction is used for:

* asynchronous workflows,
* orchestration events,
* background processing,
* manager-to-manager coordination.

---

# Extending Maestro CLI

# Adding a New CLI Command

Navigate to:

```text
cli/cmd/margo.go
```

Add the new command implementation there.

---

# Important CLI Design Detail

The CLI uses the generated SDK client from the Sandbox repository.

```text
+----------------+
| Maestro CLI    |
+----------------+
         |
         v
+----------------------+
| Generated Client SDK |
| (Sandbox)            |
+----------------------+
         |
         v
+----------------------+
| Symphony APIs        |
+----------------------+
```

This design enables:

* reusable clients,
* vendor portability,
* shared API contracts,
* SDK reuse across ecosystems.

---

# Build & Run

To build symphony locally you can use the following commands:
```bash
# to build Rust provider binding
cd api
pushd .
cd api/pkg/apis/v1alpha1/providers/target/rust
cargo build --release
popd #back to the api folder
export LIBDIR=$(pwd)/pkg/apis/v1alpha1/providers/target/rust/target/release        
CGO_ENABLED=1 GOARCH=amd64 GOOS=linux CC=gcc CGO_LDFLAGS="-L$LIBDIR" go build -o symphony-api
# copy libsymphony.so to /usr/local/lib folder
sudo cp $LIBDIR/libsymphony.so /usr/local/lib
sudo ldconfig
```

# then run it:
```bash
./symphony-agent -c ./symphony-api-margo.json -l Debug
```

To build Maestro locally you can use the following commands:
```bash
cd cli
go build -o maestro
```

# then run it:
```bash
./maestro wfm --help
```

---

# Testing

There is no internal unit tests extended here, but integration tests are available and are managed from the Sandbox repository. You can use it to verify if your changes have broken anything. If you have written something new, then extend the sanity test case in the workflow [here](https://github.com/margo/sandbox/blob/main/.github/workflows/sandbox-sanity-test.yml) .

The primary workflow is:

| Workflow              | Purpose                           |
| --------------------- | --------------------------------- |
| `sandbox-sanity-test` | End-to-end integration validation |

Workflow URL:

[Sandbox GitHub Actions Workflows](https://github.com/margo/sandbox/actions)

The workflow is intended to be triggered manually through GitHub Actions workflow dispatch. Mention your current symphony branch and corresponding sandbox branch in workflow env.

---

# Vendor Extension Recommendation

When implementing new functionality:

| Recommendation          | Guidance                                                   |
| ----------------------- | ---------------------------------------------------------- |
| Extend existing Vendors | Preferred if functionality belongs to an existing category |
| Create new Vendors      | Only when introducing an entirely new category/domain      |

This helps avoid unnecessary API fragmentation.

---

# Original Philosophy of the Sandbox Repository

The Sandbox repository was originally designed as:

* an SDK,
* a reusable client library,
* a shared contract layer,
* a reusable workflow toolkit.

That philosophy continues to shape:

* repository structure,
* generation workflows,
* client architecture,
* reusable orchestration patterns.

---

# Required Toolchain

| Tool                    | Required | Purpose                      |
| ----------------------- | -------- | ---------------------------- |
| Rust                    | Yes      | Runtime/toolchain components |
| Go                      | Yes      | Symphony and CLI development |
| Docker                  | Optional | Containerized workflows      |
| Kubernetes Distribution | Optional | Local orchestration/testing  |

---

# Suggested Contributor Mental Model

Think of the system in layers.

```text
Specification Layer
        |
        v
Generated SDK/Models
        |
        v
Vendor APIs
        |
        v
Manager Logic
        |
        v
Providers/External Systems
```

A modification at each layer affects different parts of the system:

| Layer         | Typical Impact               |
| ------------- | ---------------------------- |
| Specification | Regenerates contracts/models |
| SDK/Models    | Client compatibility         |
| Vendor        | API behavior                 |
| Manager       | Business/orchestration logic |
| Provider      | External integrations        |

---

# Final Mental Model

Margo does not deeply embed itself into Symphony internals.

Instead, it:

* layers orchestration behavior on top of Symphony,
* uses Symphony primarily as an execution/runtime substrate,
* extends the Vendor layer,
* keeps orchestration semantics largely isolated from Symphony internals.

This separation was intentional to avoid invasive rewrites of Symphony’s target-agent-oriented architecture.
