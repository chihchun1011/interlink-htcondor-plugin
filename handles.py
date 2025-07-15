import argparse
import json
import logging
import os
import re
import shlex
import subprocess
from datetime import datetime

import yaml
from flask import Flask, jsonify, request

parser = argparse.ArgumentParser()

parser.add_argument("--schedd-name", help="Schedd name", type=str, default="")
parser.add_argument("--schedd-host", help="Schedd host", type=str, default="")
parser.add_argument("--collector-host", help="Collector-host", type=str, default="")
parser.add_argument("--cadir", help="CA directory", type=str, default="")
parser.add_argument("--certfile", help="cert file", type=str, default="")
parser.add_argument("--keyfile", help="key file", type=str, default="")
parser.add_argument(
    "--auth-method", help="Default authentication methods", type=str, default=""
)
parser.add_argument("--debug", help="Debug level", type=str, default="")
parser.add_argument(
    "--condor-config", help="Path to condor_config file", type=str, default=""
)
parser.add_argument("--proxy", help="Path to proxy file", type=str, default="")
parser.add_argument(
    "--dummy-job",
    action="store_true",
    help="Whether the job should be a real job or a dummy sleep job",
)
parser.add_argument("--port", help="Server port", type=int, default=8000)

args = parser.parse_args()

if args.schedd_name != "":
    os.environ["_condor_SCHEDD_NAME"] = args.schedd_name
if args.schedd_host != "":
    os.environ["_condor_SCHEDD_HOST"] = args.schedd_host
if args.collector_host != "":
    os.environ["_condor_COLLECTOR_HOST"] = args.collector_host
if args.cadir != "":
    os.environ["_condor_AUTH_SSL_CLIENT_CADIR"] = args.cadir
if args.certfile != "":
    os.environ["_condor_AUTH_SSL_CLIENT_CERTFILE"] = args.certfile
if args.keyfile != "":
    os.environ["_condor_AUTH_SSL_CLIENT_KEYFILE"] = args.keyfile
if args.auth_method != "":
    os.environ["_condor_SEC_DEFAULT_AUTHENTICATION_METHODS"] = args.auth_method
if args.debug != "":
    os.environ["_condor_TOOL_DEBUG"] = args.debug
if args.condor_config != "":
    os.environ["CONDOR_CONFIG"] = args.condor_config
if args.proxy != "":
    os.environ["X509_USER_PROXY"] = args.proxy
if args.proxy != "":
    os.environ["X509_USER_CERT"] = args.proxy
dummy_job = args.dummy_job


global JID
JID = []


def read_yaml_file(file_path):
    with open(file_path, "r") as file:
        try:
            data = yaml.safe_load(file)
            return data
        except yaml.YAMLError as e:
            print("Error reading YAML file:", e)
            return None


global InterLinkConfigInst
interlink_config_path = "./SidecarConfig.yaml"
InterLinkConfigInst = read_yaml_file(interlink_config_path)
print("Interlink configuration info:", InterLinkConfigInst)


def error_response(message, status_code=500):
    """Create standardized error response"""
    return (
        jsonify({"error": message, "timestamp": datetime.utcnow().isoformat() + "Z"}),
        status_code,
    )


def success_response(data, status_code=200):
    """Create standardized success response"""
    return jsonify(data), status_code


def validate_pod_request(request_data):
    """Validate incoming pod request structure"""
    if not request_data:
        return False, "Empty request data"
    if not isinstance(request_data, dict):
        return False, "Request data must be a dictionary"
    if "metadata" not in request_data:
        return False, "Missing metadata in request"
    if "name" not in request_data.get("metadata", {}):
        return False, "Missing pod name in metadata"
    return True, "Valid request"


def prepare_envs(container):
    env = ""
    try:
        for env_var in container["env"]:
            if env_var.get("value") is not None:
                if env_var.get("value").startswith("["):
                    modified_value = '"' + env_var.get("value").replace('"', '"') + '"'
                    env += f"--env {env_var['name']}={modified_value} "
                else:
                    env += f"--env {env_var['name']}={env_var['value']} "
            else:
                env += f"--env {env_var['name']}= "
        return [env]
    except Exception as e:
        logging.info(f"There is some problem with your env variables: {e}")
        return [""]


def prepare_env_file(container, metadata, env_file_name="jlab.env"):
    env_file_name = f"{metadata['name']}-{metadata['uid']}_env.env"
    env_file_path = os.path.join(InterLinkConfigInst["DataRootFolder"], env_file_name)
    lines = []

    try:
        for env_var in container.get("env", []):
            name = env_var["name"]
            raw_val = env_var.get("value") or ""
            safe_val = shlex.quote(raw_val)
            lines.append(f"{name}={safe_val}")

        with open(env_file_path, "w") as fp:
            fp.write("\n".join(lines) + "\n")
        os.chmod(env_file_path, 0o777)
        logging.info(f"Wrote env file to {env_file_path}")

        return (["--env-file", env_file_name], env_file_path)

    except Exception as e:
        logging.error(f"Failed to write env file: {e}")
        return ([], None)


def prepare_mounts(pod, container_standalone):
    mounts = ["--bind"]
    mount_data = []
    pod_name = (
        container_standalone["name"].split("-")[:6]
        if len(container_standalone["name"].split("-")) > 6
        else container_standalone["name"].split("-")
    )
    pod_name_folder = os.path.join(
        InterLinkConfigInst["DataRootFolder"], "-".join(pod_name[:-1])
    )
    for c in pod["spec"]["containers"]:
        if c["name"] == container_standalone["name"]:
            container = c
    try:
        os.makedirs(pod_name_folder, exist_ok=True)
        logging.info(f"Successfully created folder {pod_name_folder}")
    except Exception as e:
        logging.error(e)
    if "volumeMounts" in container.keys():
        for mount_var in container["volumeMounts"]:
            path = ""
            for vol in pod["spec"]["volumes"]:
                if vol["name"] != mount_var["name"]:
                    continue
                if "configMap" in vol.keys():
                    config_maps_paths = mountConfigMaps(pod, container_standalone)
                    # print("bind as configmap", mount_var["name"], vol["name"])
                    for i, path in enumerate(config_maps_paths):
                        mount_data.append(path)
                elif "secret" in vol.keys():
                    secrets_paths = mountSecrets(pod, container_standalone)
                    # print("bind as secret", mount_var["name"], vol["name"])
                    for i, path in enumerate(secrets_paths):
                        mount_data.append(path)
                elif "emptyDir" in vol.keys():
                    path = mount_empty_dir(container, pod)
                    mount_data.append(path)
                elif "hostPath" in vol.keys():
                    host_path = vol["hostPath"]["path"]
                    mount_path = mount_var["mountPath"]
                    bind_path = f"{host_path}:{mount_path}"
                    mount_data.append(bind_path)
                else:
                    # Implement logic for other volume types if required.
                    logging.info("\n*********\n*To be implemented*\n********")
    else:
        logging.info("Container has no volume mount")
        return [""]

    path_hardcoded = ""
    mount_data.append(path_hardcoded)
    mounts.append(",".join(mount_data))
    print("mounts are", mounts)
    if mounts[1] == "":
        mounts = [""]
    return mounts


def extract_container(pod, container_standalone):
    for c in pod["spec"]["containers"]:
        if c["name"] == container_standalone["name"]:
            return c
    raise ValueError(f"Container {container_standalone['name']} not found in pod")


def mountConfigMaps(pod, container_standalone):
    configMapNamePaths = []
    # for c in pod["spec"]["containers"]:
    #     if c["name"] == container_standalone["name"]:
    #       container = c
    container = extract_container(pod, container_standalone)
    if InterLinkConfigInst["ExportPodData"] and "volumeMounts" in container.keys():
        data_root_folder = InterLinkConfigInst["DataRootFolder"]
        cmd = ["-rf", os.path.join(os.getcwd(), data_root_folder, "configMaps")]
        shell = subprocess.Popen(
            ["rm"] + cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )
        _, err = shell.communicate()

        if err:
            logging.error("Unable to delete root folder")

        for mountSpec in container["volumeMounts"]:
            for vol in pod["spec"]["volumes"]:
                if vol["name"] != mountSpec["name"]:
                    continue
                if "configMap" in vol.keys():
                    print("container_standalone:", container_standalone)
                    cfgMaps = container_standalone["configMaps"]
                    namespace = pod["metadata"]["namespace"]
                    uid = pod["metadata"]["uid"]
                    for cfgMap in cfgMaps:
                        podConfigMapDir = os.path.join(
                            os.getcwd(),
                            data_root_folder,
                            f"{namespace}-{uid}/configMaps/",
                            vol["name"],
                        )
                        for key in cfgMap["data"].keys():
                            path = os.path.join(os.getcwd(), podConfigMapDir, key)
                            path += f":{mountSpec['mountPath']}/{key}"
                            configMapNamePaths.append(path)
                        cmd = ["-p", podConfigMapDir]
                        shell = subprocess.Popen(
                            ["mkdir"] + cmd,
                            stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE,
                        )
                        execReturn, _ = shell.communicate()
                        if execReturn:
                            logging.error(err)
                        else:
                            logging.debug(f"--- Created folder {podConfigMapDir}")
                        logging.debug("--- Writing ConfigMaps files")
                        for k, v in cfgMap["data"].items():
                            full_path = os.path.join(podConfigMapDir, k)
                            with open(full_path, "w") as f:
                                f.write(v)
                            os.chmod(full_path, vol["configMap"]["defaultMode"])
                            logging.debug(f"--- Written ConfigMap file {full_path}")
    return configMapNamePaths


def mountSecrets(pod, container_standalone):
    secret_name_paths = []
    # for c in pod["spec"]["containers"]:
    #     if c["name"] == container_standalone["name"]:
    #         container = c
    container = extract_container(pod, container_standalone)
    if InterLinkConfigInst["ExportPodData"] and "volumeMounts" in container.keys():
        data_root_folder = InterLinkConfigInst["DataRootFolder"]
        cmd = ["-rf", os.path.join(os.getcwd(), data_root_folder, "secrets")]
        subprocess.run(["rm"] + cmd, check=True)
        for mountSpec in container["volumeMounts"]:
            for vol in pod["spec"]["volumes"]:
                if vol["name"] != mountSpec["name"]:
                    continue
                if "secret" in vol.keys():
                    secrets = container_standalone["secrets"]
                    for secret in secrets:
                        if secret["metadata"]["name"] != vol["secret"]["secretName"]:
                            continue
                        namespace = pod["metadata"]["namespace"]
                        uid = pod["metadata"]["uid"]
                        pod_secret_dir = os.path.join(
                            os.getcwd(),
                            data_root_folder,
                            f"{namespace}-{uid}/secrets/",
                            vol["name"],
                        )
                        for key in secret["data"]:
                            path = os.path.join(pod_secret_dir, key)
                            path += f":{mountSpec['mountPath']}/{key}"
                            secret_name_paths.append(path)
                        cmd = ["-p", pod_secret_dir]
                        subprocess.run(["mkdir"] + cmd, check=True)
                        logging.debug(f"--- Created folder {pod_secret_dir}")
                        logging.debug("--- Writing Secret files")
                        for k, v in secret["data"].items():
                            full_path = os.path.join(pod_secret_dir, k)
                            with open(full_path, "w") as f:
                                f.write(v)
                            os.chmod(full_path, vol["secret"]["defaultMode"])
                            logging.debug(f"--- Written Secret file {full_path}")
    return secret_name_paths


def mount_empty_dir(container, pod):
    ed_path = None
    if InterLinkConfigInst["ExportPodData"] and "volumeMounts" in container.keys():
        cmd = ["-rf", os.path.join(InterLinkConfigInst["DataRootFolder"], "emptyDirs")]
        subprocess.run(["rm"] + cmd, check=True)
        for mount_spec in container["volumeMounts"]:
            pod_volume_spec = None
            for vol in pod["spec"]["volumes"]:
                if vol.name == mount_spec["name"]:
                    pod_volume_spec = vol["volumeSource"]
                    break
            if pod_volume_spec and pod_volume_spec["EmptyDir"]:
                ed_path = os.path.join(
                    InterLinkConfigInst["DataRootFolder"],
                    pod.namespace + "-" + str(pod.uid) + "/emptyDirs/" + vol.name,
                )
                cmd = ["-p", ed_path]
                subprocess.run(["mkdir"] + cmd, check=True)
                ed_path += (
                    ":" + mount_spec["mount_path"] + "/" + mount_spec["name"] + ","
                )

    return ed_path


def parse_string_with_suffix(value_str):
    # should return MB because HTCondor wants MB
    suffixes = {
        "k": 1 / 10**3,
        "M": 1,
        "G": 10**3,
        "Ki": 1 / 1024,
        "Mi": 1,
        "Gi": 1024,
    }

    match = re.match(r"(\d+)([a-zA-Z]+)", value_str)
    if match:
        numeric_part = match.group(1)
        suffix = match.group(2)
        if suffix in suffixes:
            numeric_value = int(float(numeric_part) * suffixes[suffix])
            return numeric_value
        else:
            return 1
    else:
        print("Unrecognized memory value, setting it to 1 MB")
        return 1


def produce_htcondor_singularity_script(containers, metadata, commands, input_files):
    datarootfolder = InterLinkConfigInst["DataRootFolder"]
    name = metadata["name"]
    uid = metadata["uid"]
    executable_path = f"./{datarootfolder}/{name}-{uid}.sh"
    sub_path = f"./{datarootfolder}/{name}-{uid}.jdl"

    requested_cpus = 0
    requested_memory = 0
    for c in containers:
        if "resources" in c.keys():
            if "requests" in c["resources"].keys():
                if "cpu" in c["resources"]["requests"].keys():
                    requested_cpus += int(c["resources"]["requests"]["cpu"])
                if "memory" in c["resources"]["requests"].keys():
                    requested_memory += parse_string_with_suffix(
                        c["resources"]["requests"]["memory"]
                    )
    if requested_cpus == 0:
        requested_cpus = 1
    if requested_memory == 0:
        requested_memory = 1

    prefix_ = f"\n{InterLinkConfigInst['CommandPrefix']}"
    try:
        with open(executable_path, "w") as f:
            batch_macros = """#!/bin/bash
"""
            commands_joined = [prefix_]
            for i in range(0, len(commands)):
                if i > 0:
                    if "-c" in commands[i - 1]:
                        commands[i] = "'" + commands[i] + "'"
            commands_joined.append(" ".join(commands))
            cleaned = []
            for cmd in commands_joined:
                c = re.sub(r'\s*""\s*', " ", cmd)
                c = re.sub(r" {2,}", " ", c)
                cleaned.append(c.strip())
            commands_joined = cleaned
            f.write(batch_macros + "\n" + "\n".join(commands_joined))

        job = f"""
Executable = {executable_path}

Log        = log/mm_mul.$(Cluster).$(Process).log
Output     = out/mm_mul.out.$(Cluster).$(Process)
Error      = err/mm_mul.err.$(Cluster).$(Process)

transfer_input_files = {",".join(input_files)}
should_transfer_files = YES
RequestCpus = {requested_cpus}
RequestMemory = {requested_memory}

when_to_transfer_output = ON_EXIT_OR_EVICT
+MaxWallTimeMins = 60

+WMAgent_AgentName = "whatever"

Queue 1
"""
        # print(job)
        with open(sub_path, "w") as f_:
            f_.write(job)
        os.chmod(executable_path, 0o0777)
    except Exception as e:
        logging.error(f"Unable to prepare the job: {e}")

    return sub_path


def produce_htcondor_host_script(container, metadata):
    datarootfolder = InterLinkConfigInst["DataRootFolder"]
    name = metadata["name"]
    uid = metadata["uid"]
    executable_path = f"{datarootfolder}{name}-{uid}.sh"
    sub_path = f"{datarootfolder}{name}-{uid}.jdl"
    try:
        with open(executable_path, "w") as f:
            batch_macros = f"""#!{container['command'][-1]}
""" + "\n".join(
                container["args"][-1].split("; ")
            )

            f.write(batch_macros)

        requested_cpu = container["resources"]["requests"]["cpu"]
        # requested_memory = int(container['resources']['requests']['memory'])/1e6
        requested_memory = container["resources"]["requests"]["memory"]
        job = f"""
Executable = {executable_path}

Log        = log/mm_mul.$(Cluster).$(Process).log
Output     = out/mm_mul.out.$(Cluster).$(Process)
Error      = err/mm_mul.err.$(Cluster).$(Process)

should_transfer_files = YES
RequestCpus = {requested_cpu}
RequestMemory = {requested_memory}

when_to_transfer_output = ON_EXIT_OR_EVICT
+MaxWallTimeMins = 60

+WMAgent_AgentName = "whatever"

Queue 1
"""
        with open(sub_path, "w") as f_:
            f_.write(job)
        os.chmod(executable_path, 0o0777)
    except Exception as e:
        logging.error(f"Unable to prepare the job: {e}")

    return sub_path


def htcondor_batch_submit(job):
    logging.info("Submitting HTCondor job")
    collector = args.collector_host
    schedd = args.schedd_host
    process = os.popen(f"condor_submit -pool {collector} -remote {schedd} {job} -spool")
    preprocessed = process.read()
    process.close()
    jid = preprocessed.split(" ")[-1].split(".")[0]

    return jid


def delete_pod(pod):

    datarootfolder = InterLinkConfigInst["DataRootFolder"]
    name = pod["metadata"]["name"]
    uid = pod["metadata"]["uid"]

    logging.info(f"Deleting pod {pod['metadata']['name']}")
    with open(f"{datarootfolder}{name}-{uid}.jid") as f:
        data = f.read()
    jid = int(data.strip())
    process = os.popen(f"condor_rm {jid}")
    preprocessed = process.read()
    process.close()
    os.remove(f"{datarootfolder}{name}-{uid}.jid")
    os.remove(f"{datarootfolder}{name}-{uid}.sh")
    os.remove(f"{datarootfolder}{name}-{uid}.jdl")
    os.remove(f"{datarootfolder}{name}-{uid}_env.env")

    return preprocessed


def handle_jid(jid, pod):
    datarootfolder = InterLinkConfigInst["DataRootFolder"]
    name = pod["metadata"]["name"]
    uid = pod["metadata"]["uid"]

    with open(
        f"{datarootfolder}{name}-{uid}.jid",
        "w",
    ) as f:
        f.write(str(jid))
    JID.append({"JID": jid, "pod": pod})
    logging.info(
        f"Job {jid} submitted successfully",
        f"{datarootfolder}{name}-{uid}.jid",
    )


def SubmitHandler():
    # READ THE REQUEST ###############
    logging.info("HTCondor Sidecar: received Submit call")

    try:
        request_data_string = request.data.decode("utf-8")
        logging.debug(f"Decoded request: {request_data_string}")

        # Parse the CreateStruct (InterLink API v0.5.0+ format)
        # Format: {"pod": {...}, "container": [...]}
        create_request = json.loads(request_data_string)

        # Validate that this is a CreateStruct
        if not isinstance(create_request, dict):
            return error_response("Request must be a CreateStruct object", 400)

        if "pod" not in create_request:
            return error_response("Missing 'pod' field in request", 400)

        pod = create_request["pod"]
        containers_standalone = create_request.get("container", [])

    except json.JSONDecodeError as e:
        logging.error(f"Invalid JSON in request: {e}")
        return error_response("Invalid JSON format", 400)
    except Exception as e:
        logging.error(f"Error decoding request: {e}")
        return error_response("Error processing request", 400)

    # Validate Pod structure
    is_valid, validation_message = validate_pod_request(pod)
    if not is_valid:
        logging.error(f"Invalid Pod structure: {validation_message}")
        return error_response(f"Invalid Pod: {validation_message}", 400)

    # ELABORATE RESPONSE ###########
    # containers_standalone already extracted from create_request["container"]
    # print("Requested pod metadata name is: ", pod["metadata"]["name"])
    metadata = pod.get("metadata", {})
    containers = pod.get("spec", {}).get("containers", [])
    singularity_commands = []

    # NORMAL CASE
    if "host" not in containers[0]["image"]:
        for container in containers:
            logging.info(
                f"Beginning script generation for container {container['name']}"
            )
            commstr1 = ["singularity", "exec"]
            # envs = prepare_envs(container)
            env_flags, env_path = prepare_env_file(container, metadata)
            image = ""
            mounts = [""]
            singularity_options = metadata.get("annotations", {}).get(
                "slurm-job.vk.io/singularity-options", ""
            )

            # flags = metadata.get("annotations", {}).get(
            #     "slurm-job.vk.io/flags", "")

            pre_exec = metadata.get("annotations", {}).get(
                "slurm-job.vk.io/pre-exec", ""
            )
            if containers_standalone is not None:
                for c in containers_standalone:
                    if c["name"] == container["name"]:
                        container_standalone = c
                        mounts = prepare_mounts(pod, container_standalone)
            else:
                mounts = [""]
            # if container["image"].startswith("/") or ".io" in container["image"]:
            # if container["image"].startswith("/") or "://" in container["image"]:
            #    image_uri = metadata.get("Annotations", {}).get(
            #        "htcondor-job.knoc.io/image-root", None
            #    )
            #    if image_uri:
            #        logging.info(image_uri)
            #        image = image_uri + container["image"]
            #    else:
            #        logging.warning(
            #            "image-uri not specified for path in remote filesystem"
            #        )
            if container["image"].startswith("/cvmfs") or container["image"].startswith(
                "docker://"
            ):
                image = container["image"]
            else:
                image = "docker://" + container["image"]
            # image = container["image"]
            logging.info("Appending all commands together...")
            input_files = []
            for mount in mounts[-1].split(","):
                if "/cvmfs" not in mount:
                    input_files.append(mount.split(":")[0])
                if env_path:
                    input_files.append(env_path)
            local_mounts = ["--bind", ""]
            for mount in (mounts[-1].split(","))[:-1]:
                if "/cvmfs" not in mount:
                    prefix_ = "./"
                else:
                    prefix_ = "/"
                local_mounts[1] += (
                    prefix_
                    + (mount.split(":")[0]).split("/")[-1]
                    + ":"
                    + mount.split(":")[1]
                    + ","
                )
            if local_mounts[-1] == "":
                local_mounts = [""]

            if "command" in container.keys() and "args" in container.keys():
                singularity_command = (
                    [pre_exec]
                    + commstr1
                    + [singularity_options]
                    + env_flags
                    + local_mounts
                    + [image]
                    + container["command"]
                    + container["args"]
                )
            elif "command" in container.keys():
                singularity_command = (
                    [pre_exec]
                    + commstr1
                    + [singularity_options]
                    + env_flags
                    + local_mounts
                    + [image]
                    + container["command"]
                )
            elif "args" in container.keys():
                singularity_command = (
                    [pre_exec]
                    + commstr1
                    + [singularity_options]
                    + env_flags
                    + local_mounts
                    + [image]
                    + container["args"]
                )
            else:
                singularity_command = (
                    [pre_exec] + commstr1 + env_flags + local_mounts + [image]
                )
            # print("singularity_command:", singularity_command)
            singularity_commands.append(singularity_command)
        path = produce_htcondor_singularity_script(
            containers, metadata, singularity_command, input_files
        )

    else:
        # print("host keyword detected, ignoring other containers")
        sitename = containers[0]["image"].split(":")[-1]
        print(sitename)
        path = produce_htcondor_host_script(containers[0], metadata)

    try:
        out_jid = htcondor_batch_submit(path)
        logging.info(f"Job submitted with cluster id: {out_jid}")
        handle_jid(out_jid, pod)

        # Verify job submission was successful
        jid_file = (
            InterLinkConfigInst["DataRootFolder"]
            + pod["metadata"]["name"]
            + "-"
            + pod["metadata"]["uid"]
            + ".jid"
        )
        if not os.path.exists(jid_file):
            raise Exception("JID file was not created")

        resp = {
            "PodUID": pod["metadata"]["uid"],
            "PodJID": str(out_jid),
            "metadata": {
                "name": pod["metadata"]["name"],
                "namespace": pod["metadata"].get("namespace", "default"),
                "uid": pod["metadata"]["uid"],
            },
        }
        return success_response(resp, 201)
    except Exception as e:
        logging.error(f"Job submission failed: {e}")
        return error_response(f"Job submission failed: {str(e)}", 500)


def StopHandler():
    # READ THE REQUEST ######
    logging.info("HTCondor Sidecar: received Stop call")
    try:
        request_data_string = request.data.decode("utf-8")
        req = json.loads(request_data_string)
        # Validate request structure
        is_valid, validation_message = validate_pod_request(req)
        if not is_valid:
            logging.error(f"Invalid delete request: {validation_message}")
            return error_response(f"Invalid request: {validation_message}", 400)
    except json.JSONDecodeError as e:
        logging.error(f"Invalid JSON in delete request: {e}")
        return error_response("Invalid JSON format", 400)
    except Exception as e:
        logging.error(f"Error processing delete request: {e}")
        return error_response("Error processing request", 400)

    # DELETE JOB RELATED TO REQUEST
    try:
        return_message = delete_pod(req)
        logging.info(f"Pod deletion result: {return_message}")
        # Check if deletion was successful
        if "All" in return_message or "removed" in return_message.lower():
            resp = {
                "message": "Pod successfully deleted",
                "podUID": req.get("metadata", {}).get("uid", ""),
                "podName": req.get("metadata", {}).get("name", ""),
            }
            return success_response(resp, 200)
        else:
            return error_response("Failed to delete pod from HTCondor", 500)
    except FileNotFoundError as e:
        logging.error(f"Pod files not found during deletion: {e}")
        return error_response("Pod not found or already deleted", 404)
    except Exception as e:
        logging.error(f"Error deleting pod: {e}")
        return error_response(f"Deletion failed: {str(e)}", 500)


def StatusHandler():
    # READ THE REQUEST #####################
    logging.info("HTCondor Sidecar: received GetStatus call")
    try:
        request_data_string = request.data.decode("utf-8")
        req_list = json.loads(request_data_string)
        # Handle ping requests (empty array)
        if isinstance(req_list, list) and len(req_list) == 0:
            logging.info("Received ping request")
            if args.proxy and os.path.isfile(args.proxy):
                return success_response(
                    {"message": "HTCondor sidecar is alive", "status": "healthy"}, 200
                )
            else:
                return error_response(
                    "HTCondor sidecar not ready - proxy file not available", 503
                )
        # Validate request format
        if not isinstance(req_list, list):
            return error_response("Status request must be an array", 400)
        if len(req_list) == 0:
            return error_response("Empty request array", 400)
        req = req_list[0]
        # Validate request structure
        is_valid, validation_message = validate_pod_request(req)
        if not is_valid:
            logging.error(f"Invalid status request: {validation_message}")
            return error_response(f"Invalid request: {validation_message}", 400)
    except json.JSONDecodeError as e:
        logging.error(f"Invalid JSON in status request: {e}")
        return error_response("Invalid JSON format", 400)
    except Exception as e:
        logging.error(f"Error processing status request: {e}")
        return error_response("Error processing request", 400)

    # ELABORATE RESPONSE #################
    try:
        jid_file = (
            InterLinkConfigInst["DataRootFolder"]
            + req["metadata"]["name"]
            + "-"
            + req["metadata"]["uid"]
            + ".jid"
        )
        with open(jid_file, "r") as f:
            jid_job = f.read().strip()
        podname = req["metadata"]["name"]
        podnamespace = req["metadata"].get("namespace", "default")
        poduid = req["metadata"]["uid"]
        # Query HTCondor for job status
        process = os.popen(f"condor_q {jid_job} --json")
        preprocessed = process.read()
        process.close()
        if not preprocessed.strip():
            # Job not found in queue, check history
            process = os.popen(f"condor_history {jid_job} --json")
            preprocessed = process.read()
            process.close()
        if not preprocessed.strip():
            return error_response(
                f"Job {jid_job} not found in HTCondor queue or history", 404
            )
        job_data = json.loads(preprocessed)
        if not job_data:
            return error_response(f"No job data found for job {jid_job}", 404)
        job = job_data[0]
        status = job.get("JobStatus", 0)
        # Get actual timestamps from HTCondor
        current_time = datetime.utcnow().isoformat() + "Z"
        start_time = (
            datetime.fromtimestamp(job.get("JobStartDate", 0)).isoformat() + "Z"
            if job.get("JobStartDate")
            else current_time
        )
        completion_time = (
            datetime.fromtimestamp(job.get("CompletionDate", 0)).isoformat() + "Z"
            if job.get("CompletionDate")
            else current_time
        )
        # Map HTCondor status to Kubernetes container states
        if status == 1:  # Idle
            state = {"waiting": {"reason": "ContainerCreating"}}
            readiness = False
        elif status == 2:  # Running
            state = {"running": {"startedAt": start_time}}
            readiness = True
        elif status == 4:  # Completed
            state = {
                "terminated": {
                    "startedAt": start_time,
                    "finishedAt": completion_time,
                    "exitCode": job.get("ExitCode", 0),
                    "reason": "Completed",
                }
            }
            readiness = False
        elif status == 3:  # Removed
            state = {
                "terminated": {
                    "startedAt": start_time,
                    "finishedAt": completion_time,
                    "reason": "Cancelled",
                }
            }
            readiness = False
        elif status == 5:  # Held
            state = {
                "waiting": {
                    "reason": "JobHeld",
                    "message": job.get("HoldReason", "Job held by HTCondor"),
                }
            }
            readiness = False
        else:
            state = {"waiting": {"reason": "Unknown"}}
            readiness = False
        # Build container status list
        containers = []
        for c in req["spec"]["containers"]:
            containers.append(
                {
                    "name": c["name"],
                    "state": state,
                    "lastState": {},
                    "ready": readiness,
                    "restartCount": 0,
                    "image": c.get("image", "unknown"),
                    "imageID": c.get("image", "unknown"),
                }
            )
        # Build response in expected format
        resp = [
            {
                "name": podname,
                "UID": poduid,
                "namespace": podnamespace,
                "JID": jid_job,
                "containers": containers,
            }
        ]
        return success_response(resp, 200)
    except FileNotFoundError as e:
        logging.error(f"Job file not found: {e}")
        return error_response("Pod not found", 404)
    except json.JSONDecodeError as e:
        logging.error(f"Error parsing HTCondor response: {e}")
        return error_response("Error parsing job status", 500)
    except Exception as e:
        logging.error(f"Error retrieving pod status: {e}")
        return error_response(f"Status retrieval failed: {str(e)}", 500)


def LogsHandler():
    logging.info("HTCondor Sidecar: received GetLogs call")
    request_data_string = request.data.decode("utf-8")
    # print(request_data_string)
    req = json.loads(request_data_string)
    if req is None or not isinstance(req, dict):
        # print("Invalid logs request body is: ", req)
        logging.error("Invalid request data")
        return "Invalid request data for getting logs", 400

    resp = "NOT IMPLEMENTED"

    return json.dumps(resp), 200


app = Flask(__name__)
app.add_url_rule("/create", view_func=SubmitHandler, methods=["POST"])
app.add_url_rule("/delete", view_func=StopHandler, methods=["POST"])
app.add_url_rule("/status", view_func=StatusHandler, methods=["GET"])
app.add_url_rule("/getLogs", view_func=LogsHandler, methods=["GET"])

if __name__ == "__main__":
    app.run(port=args.port, host="0.0.0.0", debug=True)
