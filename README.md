# InterLink HTCondor Sidecar Plugin

[![InterLink Compatible](https://img.shields.io/badge/InterLink-v0.5.0+-blue)](https://github.com/interlink-hq/interLink)
[![License](https://img.shields.io/badge/license-Apache%202.0-green)](LICENSE)

This repository contains an InterLink HTCondor sidecar plugin — a container manager that interfaces with
[InterLink](https://github.com/interlink-hq/interLink) instances to deploy Kubernetes pod containers on
HTCondor batch systems using Singularity/Apptainer.

## Features

- **Full Kubernetes Pod Support**: Handles containers, volumes, secrets, configMaps, and resource requests
- **Dual Execution Modes**: Singularity containers and host-based script execution
- **InterLink API v0.5.0+ Compatible**: Modern API with proper error handling and status codes
- **Comprehensive Logging**: Aggregated stdout, stderr, and HTCondor job logs
- **Real-time Status**: Live job status with actual timestamps from HTCondor
- **Robust Error Handling**: Detailed error responses and validation

---

## Quick Start

### Prerequisites

Before you begin, make sure you have the following installed and configured:

| Requirement | Notes |
|---|---|
| Python 3.6+ | `python3 --version` |
| `pip` | `pip3 --version` |
| HTCondor command-line tools | `condor_submit`, `condor_q`, `condor_rm` must be on your `$PATH` |
| Singularity or Apptainer | Required to pull and run container images |
| Grid proxy certificate | Only for GSI authentication — run `voms-proxy-init` or `grid-proxy-init` |
| `curl` | For the quick test below |

> **Tip for beginners:** If you only want to verify that the plugin server starts and its REST API is
> reachable — without a real HTCondor cluster — you can still follow every step here.
> The `/status` health-check and the request-validation logic work without an active scheduler.

---

### 1 — Clone and install

```bash
git clone https://github.com/interlink-hq/interlink-htcondor-plugin.git
cd interlink-htcondor-plugin

# Install Python dependencies
pip3 install flask pyyaml
```

---

### 2 — Configure the plugin

Open [`SidecarConfig.yaml`](SidecarConfig.yaml) and adjust the values to your environment:

```yaml
CommandPrefix: ""            # Optional shell prefix prepended to every job command
                             # e.g. "source /cvmfs/cms.cern.ch/cmsset_default.sh;"
ExportPodData: true          # Mount ConfigMaps and Secrets into the Singularity job
DataRootFolder: ".interlink/" # Directory used to store job scripts and tracking files
```

Create the directories expected by HTCondor and the plugin:

```bash
mkdir -p .interlink out err log
```

---

### 3 — Start the plugin server

The plugin exposes a small Flask HTTP server. The simplest way to start it — no authentication required — is:

```bash
python3 handles.py \
  --schedd-host    scheduler.example.com \
  --collector-host collector.example.com \
  --port           4000
```

> **No authentication needed for local / development setups.**
> If your HTCondor pool uses `CLAIMTOBE` or no authentication, no extra flags are required.

**All available flags:**

| Flag | Description | Default |
|---|---|---|
| `--port` | TCP port the Flask server listens on | `8000` |
| `--schedd-host` | Hostname of the HTCondor scheduler | _(empty)_ |
| `--schedd-name` | HTCondor schedd name | _(empty)_ |
| `--collector-host` | Hostname of the HTCondor collector | _(empty)_ |
| `--condor-config` | Path to a custom `condor_config` file | _(empty)_ |
| `--auth-method` | HTCondor authentication method (e.g. `GSI`, `SCITOKENS`) | _(empty)_ |
| `--proxy` | Path to the X.509 proxy / SciToken file | _(empty)_ |
| `--cadir` | Directory with trusted CA certificates | _(empty)_ |
| `--certfile` | Path to the SSL client certificate | _(empty)_ |
| `--keyfile` | Path to the SSL private key | _(empty)_ |
| `--debug` | HTCondor tool debug level (e.g. `D_FULLDEBUG`) | _(empty)_ |
| `--dummy-job` | Submit a placeholder sleep job instead of the real workload | _(false)_ |

Once running you should see output similar to:

```text
 * Running on http://0.0.0.0:4000
```

The server exposes four REST endpoints:

| Method | Path | Description |
|---|---|---|
| `POST` | `/create` | Submit a new pod as an HTCondor job |
| `POST` | `/delete` | Cancel and remove a running job |
| `GET` | `/status` | Query the current status of a pod |
| `GET` | `/getLogs` | Retrieve job stdout / stderr |

---

### 4 — Quick plugin test with `curl`

The examples below target `localhost:4000`. Adjust the port if you used a different `--port` value.

#### 4a — Health check (ping)

Send an empty status array to confirm the server is alive and the proxy file is accessible:

```bash
curl -s -X GET http://localhost:4000/status \
  -H "Content-Type: application/json" \
  -d '[]' | python3 -m json.tool
```

Expected response (healthy):

```json
{
    "message": "HTCondor sidecar is alive",
    "status": "healthy"
}
```

#### 4b — Submit a test pod (`POST /create`)

The request body follows the InterLink API v0.5.0+ **CreateStruct** format: a `pod` object
(standard Kubernetes pod spec) and an optional `container` array with resolved volumes.

```bash
curl -s -X POST http://localhost:4000/create \
  -H "Content-Type: application/json" \
  -d '{
    "pod": {
      "metadata": {
        "name":      "test-pod",
        "namespace": "default",
        "uid":       "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
      },
      "spec": {
        "containers": [
          {
            "name":    "busybox",
            "image":   "docker://busybox:latest",
            "command": ["sleep", "30"],
            "resources": {
              "requests": {
                "cpu":    "1",
                "memory": "100Mi"
              }
            }
          }
        ]
      }
    },
    "container": []
  }' | python3 -m json.tool
```

On success (HTTP 201) the plugin returns the HTCondor cluster job ID:

```json
{
    "PodUID": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
    "PodJID": "12345",
    "metadata": {
        "name":      "test-pod",
        "namespace": "default",
        "uid":       "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
    }
}
```

> **Note:** The `PodJID` value (`12345` in the example) is the HTCondor cluster ID.
> You can verify the job with `condor_q 12345`.

#### 4c — Check pod status (`GET /status`)

Use the same pod metadata to query the current state:

```bash
curl -s -X GET http://localhost:4000/status \
  -H "Content-Type: application/json" \
  -d '[{
    "metadata": {
      "name":      "test-pod",
      "namespace": "default",
      "uid":       "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
    },
    "spec": {
      "containers": [
        {"name": "busybox", "image": "docker://busybox:latest"}
      ]
    }
  }]' | python3 -m json.tool
```

Example response while the job is running:

```json
[
    {
        "name": "test-pod",
        "UID":  "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
        "namespace": "default",
        "JID": "12345",
        "containers": [
            {
                "name":  "busybox",
                "state": {"running": {"startedAt": "2024-01-15T10:30:00Z"}},
                "ready": true,
                "restartCount": 0,
                "image": "docker://busybox:latest",
                "imageID": "docker://busybox:latest"
            }
        ]
    }
]
```

HTCondor job status codes map to Kubernetes container states as follows:

| HTCondor status | Code | Kubernetes state |
|---|---|---|
| Idle (queued) | 1 | `waiting` — `ContainerCreating` |
| Running | 2 | `running` |
| Completed | 4 | `terminated` — `Completed` |
| Removed | 3 | `terminated` — `Cancelled` |
| Held | 5 | `waiting` — `JobHeld` |

#### 4d — Delete a pod (`POST /delete`)

```bash
curl -s -X POST http://localhost:4000/delete \
  -H "Content-Type: application/json" \
  -d '{
    "metadata": {
      "name":      "test-pod",
      "namespace": "default",
      "uid":       "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
    }
  }' | python3 -m json.tool
```

Expected response (HTTP 200):

```json
{
    "message": "Pod successfully deleted",
    "podUID":  "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
    "podName": "test-pod"
}
```

---

### 5 — Test with Kubernetes (requires InterLink + Virtual Kubelet)

To set up a full InterLink deployment with a Virtual Kubelet node, follow the official guide:
👉 **[InterLink in-cluster setup](https://interlink-project.dev/docs/cookbook/incluster)**

Once your Virtual Kubelet node is registered and ready, you can test end-to-end by applying the
manifests in the `tests/` directory:

```bash
# Apply supporting resources first
kubectl apply -f ./tests/test_configmap.yaml
kubectl apply -f ./tests/test_secret.yaml

# Submit the test pod (mounts the ConfigMap and Secret via volume binds)
kubectl apply -f ./tests/busyecho_k8s.yaml

# Watch the pod status
kubectl get pod test-pod -w
```

The test pod runs `sleep 10` inside a `busybox` Singularity container and mounts both the ConfigMap and Secret as volumes.

#### Host-based script execution

A special execution mode is triggered when the container image name starts with the literal string
`host`. In that case the plugin extracts the script from the container arguments and submits it
directly — bypassing Singularity entirely. This is useful for site-specific scripts that must run
in the host environment:

```bash
kubectl apply -f ./tests/production_deployment_LNL.yaml
```

---

### 6 — Running with Docker

A pre-built image based on `htcondor/mini:23.0.25-el9` is available via the [`Dockerfile`](docker/Dockerfile):

```bash
# Build the image
docker build -t interlink-htcondor-plugin -f docker/Dockerfile .

# Run the container (adjust environment variables for your site)
docker run --rm -p 4000:8000 \
  -v /etc/grid-security:/etc/grid-security:ro \
  -v /tmp:/tmp \
  interlink-htcondor-plugin
```

Inside the container the plugin starts automatically (`python3 handles.py`) alongside the HTCondor mini-schedd (`/start.sh`).

---

## Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| `condor_submit: command not found` | HTCondor CLI not on `$PATH` | Install HTCondor or add its `bin/` to `$PATH` |
| HTTP 503 on health check | Proxy file missing or expired | For GSI: run `voms-proxy-init`; for SciTokens: refresh the JWT. Pass the path via `--proxy` |
| HTTP 500 on `/create` | Job submission to HTCondor failed | Check `err/` and `log/` directories for HTCondor error output |
| `FileNotFoundError: .interlink/…` | Required directories missing | Run `mkdir -p .interlink out err log` |
| Pod stuck in `ContainerCreating` | HTCondor job is queued (Idle) | Run `condor_q` to inspect the queue; check for hold reasons |
| Flask import error | Python dependencies not installed | Run `pip3 install flask pyyaml` |

---

## Appendix: Authentication

By default the plugin connects to HTCondor without any special authentication (suitable for local or
development clusters). For production deployments that require authentication, pass `--auth-method`
and the relevant credential flags.

### GSI (X.509 proxy certificates)

```bash
python3 handles.py \
  --schedd-host    scheduler.example.com \
  --collector-host collector.example.com \
  --auth-method    GSI \
  --proxy          /tmp/x509up_u$(id -u) \
  --port           4000
```

Certificates must be in `/etc/grid-security/certificates` and a valid proxy must exist at the path
provided to `--proxy`. Generate a proxy with:

```bash
voms-proxy-init --voms <your-vo>
# or
grid-proxy-init
```

### SciTokens

```bash
python3 handles.py \
  --schedd-host    scheduler.example.com \
  --collector-host collector.example.com \
  --auth-method    SCITOKENS \
  --proxy          /path/to/scitoken.jwt \
  --port           4000
```

Pass the SciToken JWT file via `--proxy`.

### SSL (mutual TLS)

```bash
python3 handles.py \
  --schedd-host    scheduler.example.com \
  --collector-host collector.example.com \
  --auth-method    SSL \
  --certfile       /path/to/client.crt \
  --keyfile        /path/to/client.key \
  --cadir          /etc/grid-security/certificates \
  --port           4000
```

---

## Repository management

All changes should go through Pull Requests.

### Merge management

- Only squash should be enforced in the repository settings.
- Update commit message for the squashed commits as needed.

### Protection on main branch

To be configured on the repository settings.

- Require pull request reviews before merging
  - Dismiss stale pull request approvals when new commits are pushed
  - Require review from Code Owners
- Require status checks to pass before merging
  - GitHub actions if available
  - Other checks as available and relevant
  - Require branches to be up to date before merging
- Include administrators
