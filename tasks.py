import os
import sys
import json
import tempfile
import zipfile
import requests
import io
from shutil import rmtree, which, copytree

from invoke.tasks import task
from galaxy.tools import zip_folder_to_file

BASE_DIR = os.path.abspath(os.path.dirname(__file__))
PROTOC_DIR = os.path.join(BASE_DIR, "protoc")

with open(os.path.join(BASE_DIR, "src", "manifest.json"), "r") as f:
    MANIFEST = json.load(f)

if sys.platform == 'win32':
    DIST_DIR = os.environ['localappdata'] + '\\GOG.com\\Galaxy\\plugins\\installed'
    PLATFORM = "win_amd64"
    
    if which("py"):
        PYTHON_EXE = "py -3.13"
    else:
        PYTHON_EXE = "python"

    PROTOC_EXE = os.path.join(PROTOC_DIR, "bin", "protoc.exe")
    PROTOC_INCLUDE_DIR = os.path.join(PROTOC_DIR, "include")
    PROTOC_DOWNLOAD_URL = "https://github.com/protocolbuffers/protobuf/releases/download/v35.1/protoc-35.1-win64.zip"


elif sys.platform == 'darwin':
    DIST_DIR = os.path.realpath(os.path.expanduser("~/Library/Application Support/GOG.com/Galaxy/plugins/installed"))
    PLATFORM = "macosx_10_13_x86_64"  # @see https://github.com/FriendsOfGalaxy/galaxy-integrations-updater/blob/master/scripts.py
    PYTHON_EXE = "python"

    PROTOC_EXE = os.path.join(PROTOC_DIR, "bin", "protoc")
    PROTOC_INCLUDE_DIR = os.path.join(PROTOC_DIR, "include")
    PROTOC_DOWNLOAD_URL = "https://github.com/protocolbuffers/protobuf/releases/download/v35.1/protoc-35.1-osx-x86_64.zip"


@task
def InstallProtoc(c):
    if os.path.exists(PROTOC_DIR) and os.path.isdir(PROTOC_DIR):
        print("protoc directory already exists, remove it if you want to reinstall protoc")
        return

    os.makedirs(PROTOC_DIR)

    resp = requests.get(PROTOC_DOWNLOAD_URL, stream=True)
    resp.raise_for_status()

    with zipfile.PyZipFile(io.BytesIO(resp.content)) as zipf:
        zipf.extractall(PROTOC_DIR)

    print("protoc successfully installed")


@task
def build(c, output='output', ziparchive=None):
    if os.path.exists(output):
        print('--> Removing {} directory'.format(output))
        rmtree(output)

    # Firstly dependencies need to be "flattened" with pip-compile,
    # as pip requires --no-deps if --platform is used.
    print('--> Flattening dependencies to temporary requirements file')
    with tempfile.NamedTemporaryFile(mode="w", delete=False) as tmp:
        c.run(f'pip-compile requirements/app.txt --resolver=backtracking --output-file=-', out_stream=tmp)

    # Then install all stuff with pip to output folder
    print('--> Installing with pip for specific version')
    args = [
        'pip', 'install',
        '-r', tmp.name,
        '--python-version', '313', # Galaxy requires Python 3.13
        '--platform', PLATFORM,
        '--target "{}"'.format(output),
        '--no-compile',
        '--no-deps',
    ]
    c.run(" ".join(args), echo=True)
    os.unlink(tmp.name)

    print('--> Copying source files')
    copytree("src", output, dirs_exist_ok=True)

    if ziparchive is not None:
        print('--> Compressing to {}'.format(ziparchive))
        zip_folder_to_file(output, ziparchive)

@task
def test(c):
    c.run('pytest')


@task
def install(c):
    dist_path = os.path.join(DIST_DIR, "origin_" + MANIFEST['guid'])
    build(c, output=dist_path)


@task
def pack(c):
    build(c, output="origin_" + MANIFEST['guid'], ziparchive='origin_v{}.zip'.format(MANIFEST['version']))
    print('--> Removing {} directory'.format("origin_" + MANIFEST['guid']))
    rmtree("origin_" + MANIFEST['guid'])

@task
def GenerateProtobufMessages(c):
    proto_files_dir = os.path.join(BASE_DIR, "src", "rtm_protos")

    out_dir = os.path.join(BASE_DIR, "src", "generated_protos")

    try:
        rmtree(os.path.join(out_dir))
    except Exception:
        pass  # directory probably just didn't exist

    os.makedirs(os.path.join(out_dir), exist_ok=True)

    # make sure __init__.py is there
    with open(os.path.join(out_dir, "__init__.py"), "wb") as fp:
        fp.write(b"")

    all_files = " ".join(map(lambda x: '"' + os.path.join(proto_files_dir, x) + '"', os.listdir(proto_files_dir)))
    print(f'"{PROTOC_EXE}" -I "{proto_files_dir}" --python_out="{out_dir}" {all_files}')
    c.run(f'"{PROTOC_EXE}" -I "{proto_files_dir}" --python_out="{out_dir}" {all_files}')

