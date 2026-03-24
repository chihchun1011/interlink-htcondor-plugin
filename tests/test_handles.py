"""
Unit tests for the prepare_probes function and related prepare-* logic
in handles.py.

prepare_probes follows the same pattern as prepare_env_file and
prepare_mounts: it is called once per container inside SubmitHandler and
returns (probe_script, cleanup_script) strings ready to be injected into
the job executable by produce_htcondor_singularity_script.
"""

import os
import sys
import tempfile
import types
import unittest

import pytest

# ---------------------------------------------------------------------------
# Minimal stubs so handles.py can be imported without a real config file or
# HTCondor installation.
# ---------------------------------------------------------------------------

# Stub out yaml / flask before importing handles so that argument parsing and
# config reading don't run at import time.
import importlib
import unittest.mock as mock


def _make_handles_module():
    """Import handles with mocked globals so the top-level code doesn't fail."""

    # Patch sys.argv so argparse doesn't consume pytest arguments.
    with mock.patch("sys.argv", ["handles.py"]):
        # Ensure the repo root is on sys.path for the relative import of probes.
        repo_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        if repo_root not in sys.path:
            sys.path.insert(0, repo_root)

        # We need a real SidecarConfig.yaml – use the one in the repo root.
        orig_dir = os.getcwd()
        os.chdir(repo_root)
        try:
            import handles as h
        finally:
            os.chdir(orig_dir)
    return h


# Import once for the whole module.
handles = _make_handles_module()
prepare_probes = handles.prepare_probes


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_BASE_METADATA = {
    "name": "test-pod",
    "uid": "abc-123",
    "namespace": "default",
    "annotations": {},
}


def _container(name="c1", image="busybox:latest", **probes):
    c = {"name": name, "image": image}
    c.update(probes)
    return c


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestPrepareProbesNoProbes:
    def test_returns_empty_strings_when_no_probes(self):
        container = _container()
        probe_script, cleanup_script = prepare_probes(container, _BASE_METADATA)
        assert probe_script == ""
        assert cleanup_script == ""

    def test_returns_empty_strings_when_probes_null(self):
        container = _container(livenessProbe=None, readinessProbe=None, startupProbe=None)
        probe_script, cleanup_script = prepare_probes(container, _BASE_METADATA)
        assert probe_script == ""
        assert cleanup_script == ""


class TestPrepareProbesHTTP:
    def test_http_liveness_probe(self):
        container = _container(
            livenessProbe={"httpGet": {"path": "/live", "port": 8080}}
        )
        probe_script, cleanup_script = prepare_probes(container, _BASE_METADATA)
        assert probe_script != ""
        assert '"liveness"' in probe_script
        assert "executeHTTPProbe()" in probe_script

    def test_http_readiness_probe(self):
        container = _container(
            readinessProbe={"httpGet": {"path": "/ready", "port": 9090}}
        )
        probe_script, cleanup_script = prepare_probes(container, _BASE_METADATA)
        assert probe_script != ""
        assert '"readiness"' in probe_script
        assert "9090" in probe_script

    def test_http_startup_probe(self):
        container = _container(
            startupProbe={"httpGet": {"path": "/start", "port": 3000}}
        )
        probe_script, cleanup_script = prepare_probes(container, _BASE_METADATA)
        assert probe_script != ""
        assert '"startup"' in probe_script

    def test_all_three_http_probes(self):
        container = _container(
            livenessProbe={"httpGet": {"path": "/live", "port": 8080}},
            readinessProbe={"httpGet": {"path": "/ready", "port": 8080}},
            startupProbe={"httpGet": {"path": "/start", "port": 8080}},
        )
        probe_script, cleanup_script = prepare_probes(container, _BASE_METADATA)
        assert '"liveness"' in probe_script
        assert '"readiness"' in probe_script
        assert '"startup"' in probe_script

    def test_cleanup_script_contains_trap(self):
        container = _container(
            livenessProbe={"httpGet": {"path": "/live", "port": 8080}}
        )
        _, cleanup_script = prepare_probes(container, _BASE_METADATA)
        assert "trap cleanup_probes EXIT" in cleanup_script

    def test_cleanup_script_contains_pid_var(self):
        container = _container(
            readinessProbe={"httpGet": {"path": "/ready", "port": 8080}}
        )
        _, cleanup_script = prepare_probes(container, _BASE_METADATA)
        assert "READINESS_PROBE_c1_0_PID" in cleanup_script


class TestPrepareProbesExec:
    def test_exec_liveness_probe(self):
        container = _container(
            livenessProbe={"exec": {"command": ["/bin/sh", "-c", "echo ok"]}}
        )
        probe_script, _ = prepare_probes(container, _BASE_METADATA)
        assert "executeExecProbe()" in probe_script
        assert '"liveness"' in probe_script

    def test_exec_readiness_probe(self):
        container = _container(
            readinessProbe={"exec": {"command": ["cat", "/tmp/ready"]}}
        )
        probe_script, _ = prepare_probes(container, _BASE_METADATA)
        assert '"readiness"' in probe_script
        assert '"cat"' in probe_script


class TestPrepareProbesImageHandling:
    def test_plain_image_gets_docker_prefix(self):
        container = _container(
            image="myrepo/myimage:latest",
            livenessProbe={"httpGet": {"port": 8080}},
        )
        probe_script, _ = prepare_probes(container, _BASE_METADATA)
        assert '"docker://myrepo/myimage:latest"' in probe_script

    def test_docker_image_not_double_prefixed(self):
        container = _container(
            image="docker://busybox:latest",
            livenessProbe={"httpGet": {"port": 8080}},
        )
        probe_script, _ = prepare_probes(container, _BASE_METADATA)
        assert '"docker://busybox:latest"' in probe_script
        assert "docker://docker://" not in probe_script

    def test_cvmfs_image_not_prefixed(self):
        container = _container(
            image="/cvmfs/atlas.cern.ch/repo/containers/fs/singularity/x86_64-centos7",
            livenessProbe={"httpGet": {"port": 8080}},
        )
        probe_script, _ = prepare_probes(container, _BASE_METADATA)
        assert "docker://" not in probe_script


class TestPrepareProbesAnnotations:
    def test_singularity_options_from_annotation(self):
        metadata = {
            **_BASE_METADATA,
            "annotations": {"slurm-job.vk.io/singularity-options": "--nv --bind /scratch"},
        }
        container = _container(
            livenessProbe={"exec": {"command": ["true"]}}
        )
        probe_script, _ = prepare_probes(container, metadata)
        assert '"--nv"' in probe_script
        assert '"--bind"' in probe_script

    def test_custom_singularity_path_from_config(self):
        orig = handles.InterLinkConfigInst.get("SingularityPath")
        handles.InterLinkConfigInst["SingularityPath"] = "/opt/singularity/bin/singularity"
        try:
            container = _container(
                livenessProbe={"exec": {"command": ["true"]}}
            )
            probe_script, _ = prepare_probes(container, _BASE_METADATA)
            assert '"/opt/singularity/bin/singularity"' in probe_script
        finally:
            if orig is None:
                handles.InterLinkConfigInst.pop("SingularityPath", None)
            else:
                handles.InterLinkConfigInst["SingularityPath"] = orig

    def test_no_singularity_path_defaults_to_singularity(self):
        orig = handles.InterLinkConfigInst.pop("SingularityPath", None)
        try:
            container = _container(
                livenessProbe={"exec": {"command": ["true"]}}
            )
            probe_script, _ = prepare_probes(container, _BASE_METADATA)
            assert '"singularity"' in probe_script
        finally:
            if orig is not None:
                handles.InterLinkConfigInst["SingularityPath"] = orig


class TestPrepareProbesContainerNameNormalisation:
    def test_dashes_replaced_in_pid_var(self):
        container = _container(
            name="my-fancy-container",
            livenessProbe={"httpGet": {"port": 8080}},
        )
        _, cleanup_script = prepare_probes(container, _BASE_METADATA)
        assert "LIVENESS_PROBE_my_fancy_container_0_PID" in cleanup_script

    def test_probe_script_uses_normalised_var(self):
        container = _container(
            name="foo-bar",
            readinessProbe={"httpGet": {"port": 9000}},
        )
        probe_script, _ = prepare_probes(container, _BASE_METADATA)
        assert "READINESS_PROBE_foo_bar_0_PID" in probe_script


class TestPrepareProbesReturnTypes:
    def test_returns_tuple_of_two_strings(self):
        container = _container(
            livenessProbe={"httpGet": {"port": 8080}}
        )
        result = prepare_probes(container, _BASE_METADATA)
        assert isinstance(result, tuple)
        assert len(result) == 2
        assert isinstance(result[0], str)
        assert isinstance(result[1], str)

    def test_both_strings_nonempty_when_probe_defined(self):
        container = _container(
            readinessProbe={"exec": {"command": ["true"]}}
        )
        probe_script, cleanup_script = prepare_probes(container, _BASE_METADATA)
        assert probe_script
        assert cleanup_script
