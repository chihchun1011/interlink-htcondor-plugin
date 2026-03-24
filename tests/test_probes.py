"""
Unit tests for the probes module.

Tests cover:
- translate_kubernetes_probes: parsing liveness/readiness/startup probes from
  container dicts (mirroring the Kubernetes API JSON representation used by
  the interLink plugin).
- generate_probe_script: verifying that the generated bash script contains the
  expected function definitions and probe invocation lines.
- generate_probe_cleanup_script: verifying the cleanup/trap section.
"""

import pytest

from probes import (
    PROBE_TYPE_EXEC,
    PROBE_TYPE_HTTP,
    ExecAction,
    HTTPGetAction,
    ProbeCommand,
    _build_probe_args,
    _translate_single_probe,
    generate_probe_cleanup_script,
    generate_probe_script,
    translate_kubernetes_probes,
)


# ---------------------------------------------------------------------------
# _translate_single_probe
# ---------------------------------------------------------------------------


class TestTranslateSingleProbe:
    def test_none_returns_none(self):
        assert _translate_single_probe(None) is None

    def test_http_probe_basic(self):
        k8s = {
            "httpGet": {"path": "/healthz", "port": 8080, "scheme": "HTTP"},
            "initialDelaySeconds": 5,
            "periodSeconds": 10,
            "timeoutSeconds": 2,
            "successThreshold": 1,
            "failureThreshold": 3,
        }
        probe = _translate_single_probe(k8s)
        assert probe is not None
        assert probe.type == PROBE_TYPE_HTTP
        assert probe.http_get.path == "/healthz"
        assert probe.http_get.port == 8080
        assert probe.http_get.scheme == "HTTP"
        assert probe.http_get.host == "localhost"
        assert probe.initial_delay_seconds == 5
        assert probe.period_seconds == 10
        assert probe.timeout_seconds == 2
        assert probe.success_threshold == 1
        assert probe.failure_threshold == 3

    def test_http_probe_defaults(self):
        """Missing fields should fall back to Kubernetes defaults."""
        probe = _translate_single_probe({"httpGet": {"port": 9090}})
        assert probe is not None
        assert probe.type == PROBE_TYPE_HTTP
        assert probe.http_get.path == "/"
        assert probe.http_get.scheme == "HTTP"
        assert probe.http_get.host == "localhost"
        assert probe.period_seconds == 10
        assert probe.timeout_seconds == 1
        assert probe.success_threshold == 1
        assert probe.failure_threshold == 3

    def test_exec_probe_basic(self):
        k8s = {
            "exec": {"command": ["/bin/sh", "-c", "cat /tmp/healthy"]},
            "initialDelaySeconds": 0,
            "periodSeconds": 5,
        }
        probe = _translate_single_probe(k8s)
        assert probe is not None
        assert probe.type == PROBE_TYPE_EXEC
        assert probe.exec_action.command == ["/bin/sh", "-c", "cat /tmp/healthy"]

    def test_unsupported_probe_returns_none(self, caplog):
        """TCP socket probes are not supported and should return None with a warning."""
        import logging

        k8s = {"tcpSocket": {"port": 8080}}
        with caplog.at_level(logging.WARNING):
            probe = _translate_single_probe(k8s)
        assert probe is None
        assert "Unsupported probe type" in caplog.text


# ---------------------------------------------------------------------------
# translate_kubernetes_probes
# ---------------------------------------------------------------------------


class TestTranslateKubernetesProbes:
    def _make_container(self, **kwargs):
        base = {"name": "test-container", "image": "busybox:latest"}
        base.update(kwargs)
        return base

    def test_no_probes(self):
        container = self._make_container()
        r, l, s = translate_kubernetes_probes(container)
        assert r == [] and l == [] and s == []

    def test_liveness_probe(self):
        container = self._make_container(
            livenessProbe={"httpGet": {"path": "/live", "port": 8080}}
        )
        r, l, s = translate_kubernetes_probes(container)
        assert len(l) == 1
        assert l[0].type == PROBE_TYPE_HTTP
        assert r == [] and s == []

    def test_readiness_probe(self):
        container = self._make_container(
            readinessProbe={"exec": {"command": ["cat", "/tmp/ready"]}}
        )
        r, l, s = translate_kubernetes_probes(container)
        assert len(r) == 1
        assert r[0].type == PROBE_TYPE_EXEC
        assert l == [] and s == []

    def test_startup_probe(self):
        container = self._make_container(
            startupProbe={"httpGet": {"path": "/startup", "port": 3000}}
        )
        r, l, s = translate_kubernetes_probes(container)
        assert len(s) == 1
        assert s[0].type == PROBE_TYPE_HTTP
        assert r == [] and l == []

    def test_all_three_probes(self):
        container = self._make_container(
            livenessProbe={"httpGet": {"path": "/live", "port": 8080}},
            readinessProbe={"httpGet": {"path": "/ready", "port": 8080}},
            startupProbe={"exec": {"command": ["/bin/check-startup"]}},
        )
        r, l, s = translate_kubernetes_probes(container)
        assert len(r) == 1 and len(l) == 1 and len(s) == 1

    def test_null_probe_value_ignored(self):
        """A probe key explicitly set to None should be treated as absent."""
        container = self._make_container(livenessProbe=None)
        r, l, s = translate_kubernetes_probes(container)
        assert l == []


# ---------------------------------------------------------------------------
# _build_probe_args
# ---------------------------------------------------------------------------


class TestBuildProbeArgs:
    def test_http_args(self):
        probe = ProbeCommand(
            probe_type=PROBE_TYPE_HTTP,
            timeout_seconds=3,
            http_get=HTTPGetAction(scheme="HTTPS", host="myhost", port=9443, path="/check"),
        )
        args = _build_probe_args(probe)
        assert '"HTTPS"' in args
        assert '"myhost"' in args
        assert "9443" in args
        assert '"/check"' in args
        assert "3" in args

    def test_exec_args(self):
        probe = ProbeCommand(
            probe_type=PROBE_TYPE_EXEC,
            exec_action=ExecAction(command=["/bin/sh", "-c", "echo ok"]),
        )
        args = _build_probe_args(probe)
        assert '"/bin/sh"' in args
        assert '"-c"' in args
        assert '"echo ok"' in args

    def test_unknown_type_returns_empty(self):
        probe = ProbeCommand(probe_type="unknown")
        assert _build_probe_args(probe) == ""


# ---------------------------------------------------------------------------
# generate_probe_script
# ---------------------------------------------------------------------------


class TestGenerateProbeScript:
    IMAGE = "docker://busybox:latest"
    CONTAINER = "my-container"

    def _http_probe(self, **kwargs):
        defaults = dict(
            probe_type=PROBE_TYPE_HTTP,
            initial_delay_seconds=0,
            period_seconds=10,
            timeout_seconds=1,
            success_threshold=1,
            failure_threshold=3,
            http_get=HTTPGetAction(port=8080, path="/health"),
        )
        defaults.update(kwargs)
        return ProbeCommand(**defaults)

    def _exec_probe(self, command=None):
        return ProbeCommand(
            probe_type=PROBE_TYPE_EXEC,
            exec_action=ExecAction(command=command or ["cat", "/tmp/ok"]),
        )

    def test_no_probes_returns_empty(self):
        result = generate_probe_script(self.CONTAINER, self.IMAGE, [], [], [])
        assert result == ""

    def test_script_contains_helper_functions(self):
        probe = self._http_probe()
        script = generate_probe_script(self.CONTAINER, self.IMAGE, [probe], [], [])
        assert "executeHTTPProbe()" in script
        assert "executeExecProbe()" in script
        assert "runProbe()" in script
        assert "waitForProbes()" in script

    def test_readiness_probe_invocation(self):
        probe = self._http_probe()
        script = generate_probe_script(self.CONTAINER, self.IMAGE, [probe], [], [])
        assert '"readiness"' in script
        assert "READINESS_PROBE_my_container_0_PID" in script

    def test_liveness_probe_invocation(self):
        probe = self._http_probe()
        script = generate_probe_script(self.CONTAINER, self.IMAGE, [], [probe], [])
        assert '"liveness"' in script
        assert "LIVENESS_PROBE_my_container_0_PID" in script

    def test_startup_probe_invocation(self):
        probe = self._http_probe()
        script = generate_probe_script(self.CONTAINER, self.IMAGE, [], [], [probe])
        assert '"startup"' in script
        assert "STARTUP_PROBE_my_container_0_PID" in script
        assert "runStartupProbe()" in script

    def test_exec_probe_uses_singularity_exec(self):
        probe = self._exec_probe()
        script = generate_probe_script(
            self.CONTAINER,
            self.IMAGE,
            [probe],
            [],
            [],
            singularity_path="/usr/bin/singularity",
        )
        assert '"/usr/bin/singularity"' in script
        assert "exec" in script

    def test_http_probe_args_in_script(self):
        probe = self._http_probe(
            http_get=HTTPGetAction(scheme="HTTP", host="localhost", port=8080, path="/healthz")
        )
        script = generate_probe_script(self.CONTAINER, self.IMAGE, [probe], [], [])
        assert "8080" in script
        assert "/healthz" in script

    def test_container_name_with_dashes_normalised_in_var(self):
        """Dashes in container names must be replaced with underscores in variable names."""
        probe = self._http_probe()
        script = generate_probe_script("my-fancy-container", self.IMAGE, [probe], [], [])
        assert "READINESS_PROBE_my_fancy_container_0_PID" in script

    def test_orchestration_sub_shell_present(self):
        """All probe invocations are wrapped inside a background sub-shell."""
        probe = self._http_probe()
        script = generate_probe_script(self.CONTAINER, self.IMAGE, [probe], [], [])
        # Sub-shell is opened with ( and backgrounded with ) &
        assert ") &" in script

    def test_singularity_options_included(self):
        probe = self._exec_probe()
        script = generate_probe_script(
            self.CONTAINER,
            self.IMAGE,
            [probe],
            [],
            [],
            singularity_options=["--nv", "--bind", "/scratch"],
        )
        assert '"--nv"' in script
        assert '"--bind"' in script

    def test_no_startup_message_when_no_startup(self):
        """When no startup probes are defined the no-startup fallback text is present."""
        probe = self._http_probe()
        script = generate_probe_script(self.CONTAINER, self.IMAGE, [probe], [], [])
        assert "No startup probes defined" in script

    def test_all_probes_together(self):
        r = self._http_probe()
        l = self._http_probe()
        s = self._exec_probe()
        script = generate_probe_script(self.CONTAINER, self.IMAGE, [r], [l], [s])
        assert "startup" in script
        assert "readiness" in script
        assert "liveness" in script
        assert "STARTUP_PROBE_my_container_0_PID" in script
        assert "READINESS_PROBE_my_container_0_PID" in script
        assert "LIVENESS_PROBE_my_container_0_PID" in script


# ---------------------------------------------------------------------------
# generate_probe_cleanup_script
# ---------------------------------------------------------------------------


class TestGenerateProbeCleanupScript:
    IMAGE = "docker://busybox:latest"
    CONTAINER = "my-container"

    def _http_probe(self):
        return ProbeCommand(
            probe_type=PROBE_TYPE_HTTP,
            http_get=HTTPGetAction(port=8080),
        )

    def test_no_probes_returns_empty(self):
        result = generate_probe_cleanup_script(self.CONTAINER, [], [], [])
        assert result == ""

    def test_contains_cleanup_function(self):
        probe = self._http_probe()
        script = generate_probe_cleanup_script(self.CONTAINER, [probe], [], [])
        assert "cleanup_probes()" in script

    def test_contains_trap(self):
        probe = self._http_probe()
        script = generate_probe_cleanup_script(self.CONTAINER, [probe], [], [])
        assert "trap cleanup_probes EXIT" in script

    def test_kills_readiness_probe_pid(self):
        probe = self._http_probe()
        script = generate_probe_cleanup_script(self.CONTAINER, [probe], [], [])
        assert "READINESS_PROBE_my_container_0_PID" in script

    def test_kills_liveness_probe_pid(self):
        probe = self._http_probe()
        script = generate_probe_cleanup_script(self.CONTAINER, [], [probe], [])
        assert "LIVENESS_PROBE_my_container_0_PID" in script

    def test_kills_startup_probe_pid(self):
        probe = self._http_probe()
        script = generate_probe_cleanup_script(self.CONTAINER, [], [], [probe])
        assert "STARTUP_PROBE_my_container_0_PID" in script

    def test_multiple_probes_all_pids_present(self):
        probes = [self._http_probe(), self._http_probe()]
        script = generate_probe_cleanup_script(self.CONTAINER, probes, [], [])
        assert "READINESS_PROBE_my_container_0_PID" in script
        assert "READINESS_PROBE_my_container_1_PID" in script
