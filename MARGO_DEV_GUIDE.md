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

## Standard and Non-Standard API Bindings

The Margo deployment uses separate HTTP bindings for standard Margo APIs and non-standard extension APIs. This is a change from the earlier setup, where a single server-side TLS binding on port `8082` hosted both API categories.

The default configuration in `api/symphony-api-margo.json` contains:

* Port `8082`: regular TLS, used for non-standard extension APIs and other endpoints that are not reserved for standard Margo APIs. Its server certificate is configured with `certs.localfile`.
* Port `8084`: MIAF mTLS, used for standard Margo API routes. Its WFM SVID and key are configured with `certs.localfile`.

This separation is implemented by the COA host. Routes beginning with `margo/api/v1` are standard Margo API routes and are exposed on an mTLS binding. Routes beginning with `margo/nbi/v1` are non-standard extension API routes and remain on the non-mTLS binding. Other Symphony routes are available on both binding types. Consequently, an mTLS binding must not be treated as a general-purpose second API listener: it is intended for standard Margo APIs. Operators are discouraged from configuring more than one mTLS binding unless they understand the resulting route and certificate behavior.

The binding split does not change Symphony's HB-MVP architecture or the separation between Vendors, Managers, and Providers. It adds a transport and authorization boundary around standard Margo APIs.

## MIAF Configuration

When `mtls` is `true`, the binding must include a valid `miaf` configuration. MIAF connects the WFM's standard Margo APIs to the Margo Identity Service (MIS), establishes the trust material used for SPIFFE identity verification, and supplies the local authorization policy for WFM clients.

The fields have the following meanings:

| Field | Meaning and requirements |
| ----- | ------------------------ |
| `miaf.mis.endpoint` | MIS endpoint used to obtain identity/trust information. It is optional only when a static trust bundle is supplied. |
| `miaf.mis.caPath` | CA certificate used to establish TLS trust when connecting to MIS. It must be provided together with `endpoint`; supplying only one of the two is invalid. |
| `miaf.mis.cacheInterval` | Refresh interval, in seconds, for the MIS/trust-material cacher. The COA HTTP binding passes this value to the cacher. |
| `miaf.mis.trustDomain` | SPIFFE trust domain. It is required when using a static `trustBundle.path` without an MIS `endpoint` and `caPath`. |
| `miaf.mis.trustBundle.uri` | URI used to obtain a trust bundle through MIS. If present, `endpoint` and `caPath` are also required. |
| `miaf.mis.trustBundle.path` | Local static trust-bundle JSON file. It can be used instead of MIS endpoint access, but `trustDomain` is then required. |
| `miaf.authzPath` | Local JSON file containing the SPIFFE IDs of clients authorized to call standard Margo APIs. The listed IDs must be WFM-client identities. |

The configuration may omit optional fields when the deployment does not use them. The current validation rules require `miaf` and `miaf.mis`; require `endpoint` and `caPath` together; require either MIS access or a static trust-bundle path; and require `trustDomain` for static trust-bundle-only mode. The `certProvider` on an mTLS binding is separate from the MIS trust configuration: it supplies the WFM's own SVID certificate and private key, and currently must use `certs.localfile`.

### How MIAF mTLS Works in Symphony

At startup, the COA host selects only the standard Margo API endpoints for an mTLS binding, validates and parses the MIAF configuration, and validates every configured authorized client SPIFFE ID. The HTTP binding then:

1. Loads and validates the WFM's own X.509 SVID and private key.
2. Starts a trust-material cacher backed by MIS and/or the configured trust bundle.
3. Starts an authorization cacher backed by `miaf.authzPath`.
4. Builds an mTLS server configuration that verifies client certificates against the current trust bundle and trust domain.
5. For each request, extracts the peer certificate's SPIFFE ID and checks it against the authorized-client list before invoking the endpoint handler.

Requests without a TLS connection, without a peer certificate, with an invalid SPIFFE ID, or with a SPIFFE ID absent from the authorization file receive HTTP `401 Unauthorized`. The mTLS binding therefore provides both mutual certificate authentication and an explicit local client allow-list; possession of a certificate trusted by the trust bundle alone is not sufficient.

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

Initially, the implementation attempted to convert Margo objects into Symphony's internal state model in order to reuse Symphony's state management system.

However, this approach was eventually abandoned because:

* Symphony's internal target agent assumptions became difficult to preserve,
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

The Margo configuration uses relative paths. Unless absolute paths are placed in the configuration, start the binary with the working directory set to `api/`, or change the paths in `api/symphony-api-margo.json` accordingly.

Before starting Symphony with `symphony-api-margo.json`, the following files must exist on disk:

| Path, relative to the process working directory | Purpose |
| ----------------------------------------------- | ------- |
| `certificates/server-cert.pem` | Server certificate for the regular TLS binding on port `8082`. |
| `certificates/server-key.pem` | Private key for the port `8082` server certificate. |
| `certificates/payload-cert.pem` | WFM X.509 SVID certificate for the MIAF mTLS binding on port `8084`. It must be a valid WFM SVID. |
| `certificates/payload-key.pem` | Private key matching the WFM SVID. It must be a valid private key. |
| `mis/https-ca.crt` | CA certificate used to verify the TLS connection from the WFM to the MIS endpoint. |
| `mis/authorized-clients.json` | Authorized WFM-client SPIFFE IDs. The file must contain at least one valid WFM-client ID. |
| `libsymphony.so` | Rust provider shared library. It must be installed in a system library directory or available through `LD_LIBRARY_PATH`; it is not embedded in `symphony-api`. |

The active configuration obtains the SPIFFE trust bundle from `https://mis.margo.org:9443` using the configured MIS endpoint and `mis/https-ca.crt`. Therefore, a local `trustBundle.path` file is not required for the active configuration. A static trust bundle can be used instead by setting `miaf.mis.trustBundle.path`; in that mode, the file must exist, `miaf.mis.trustDomain` must be provided, and MIS endpoint access can be omitted according to the MIAF validation rules. When `trustBundle.uri` is configured, the MIS endpoint and CA path are required.

The Rust provider shared library must also be available at runtime. It is not part of the Go binary. After building it, either copy `libsymphony.so` to a system library directory such as `/usr/local/lib` and run `ldconfig`, or expose its directory through `LD_LIBRARY_PATH`.

The expected local layout is:

```text
api/
├── symphony-api
├── symphony-api-margo.json
├── certificates/
│   ├── server-cert.pem
│   ├── server-key.pem
│   ├── payload-cert.pem
│   └── payload-key.pem
└── mis/
        ├── https-ca.crt
        └── authorized-clients.json
```

The shared library may be outside this directory when it is installed system-wide or referenced through `LD_LIBRARY_PATH`.

To build Symphony locally, run the following commands from the repository root:

```bash
# to build Rust provider binding
cd api
pushd .
cd pkg/apis/v1alpha1/providers/target/rust
cargo build --release
popd #back to the api folder
export LIBDIR=$(pwd)/pkg/apis/v1alpha1/providers/target/rust/target/release  
CGO_ENABLED=1 GOARCH=amd64 GOOS=linux CC=gcc CGO_LDFLAGS="-L$LIBDIR" go build -o symphony-api
# copy libsymphony.so to /usr/local/lib folder
sudo cp $LIBDIR/libsymphony.so /usr/local/lib
sudo ldconfig
```

Run the API from the `api/` directory so the relative MIAF paths resolve correctly:

```bash
cd api
./symphony-api -c ./symphony-api-margo.json -l Debug
```

If the shared library is not installed system-wide, launch it with:

```bash
LD_LIBRARY_PATH="$PWD/pkg/apis/v1alpha1/providers/target/rust/target/release" ./symphony-api -c ./symphony-api-margo.json -l Debug
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

This separation was intentional to avoid invasive rewrites of Symphony's target-agent-oriented architecture.
