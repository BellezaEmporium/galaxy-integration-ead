import os
import sys
import json
import tempfile
import requests
import io
from shutil import rmtree, which
from distutils.dir_util import copy_tree

from invoke.tasks import task
from galaxy.tools import zip_folder_to_file

BASE_DIR = os.path.abspath(os.path.dirname(__file__))
PROTOS_URL = "https://github.com/BellezaEmporium/EA-Protobuffers/archive/refs/heads/feat/updater.zip"

with open(os.path.join(BASE_DIR, "src", "manifest.json"), "r") as f:
    MANIFEST = json.load(f)

if sys.platform == 'win32':
    DIST_DIR = os.environ['localappdata'] + '\\GOG.com\\Galaxy\\plugins\\installed'
    PLATFORM = "win32"
    
    if which("py"):
        PYTHON_EXE = "py -3.7"
    else:
        PYTHON_EXE = "python"


elif sys.platform == 'darwin':
    DIST_DIR = os.path.realpath(os.path.expanduser("~/Library/Application Support/GOG.com/Galaxy/plugins/installed"))
    PLATFORM = "macosx_10_13_x86_64"  # @see https://github.com/FriendsOfGalaxy/galaxy-integrations-updater/blob/master/scripts.py
    PYTHON_EXE = "python"


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
        '--python-version', '37', # Galaxy requires Python 3.7
        '--platform', PLATFORM,
        '--target "{}"'.format(output),
        '--no-compile',
        '--no-deps'
    ]
    c.run(" ".join(args), echo=True)
    os.unlink(tmp.name)

    print('--> Copying source files')
    copy_tree("src", output)

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
def download_protos(c):
    """Download the latest protobuf files from the remote repository."""
    file = io.BytesIO()
    print(f"Downloading protos from {PROTOS_URL}...")
    try:
        response = requests.get(PROTOS_URL)
        response.raise_for_status()
        file.write(response.content)
        file.seek(0)
        print("Download complete.")
    except requests.RequestException as e:
        print(f"Failed to download protos: {e}")
        return
    
    import zipfile
    with zipfile.ZipFile(file) as z:
        # find the root folder in the zip (there should be only one)
        root_folder = None
        for name in z.namelist():
            if name.endswith('/') and not root_folder:
                root_folder = name
                break
        
        if not root_folder:
            print("Could not find root folder in the zip file.")
            return
        
        # extract the zip file to a temporary directory
        with tempfile.TemporaryDirectory() as tmpdir:
            z.extractall(tmpdir)
            extracted_protos_path = os.path.join(tmpdir, root_folder, "protos")
            
            if not os.path.isdir(extracted_protos_path):
                print("Could not find proto folder in the extracted files.")
                return
            
            target_protos_path = os.path.join(BASE_DIR, "src", "ea_protos")
            
            # remove existing protos if they exist
            if os.path.exists(target_protos_path):
                rmtree(target_protos_path)
            
            # move the new protos to the target location
            copy_tree(extracted_protos_path, target_protos_path)
            print(f"Protobuf files have been updated in {target_protos_path}.")
        

@task
def generate_protos(c, files=None, all=False):
    """Generate Python protobuf modules from .proto files in src/ea_protos.

    Usage:
      invoke generate_protos
      invoke generate_protos --files="Server.proto,Messages.proto"
      invoke generate_protos --all
    """
    proto_root = os.path.join(BASE_DIR, 'src', 'ea_protos')
    if not os.path.isdir(proto_root):
        print(f"Proto root not found: {proto_root}")
        return

    if all:
        protos = []
        for root, dirs, filenames in os.walk(proto_root):
            for fn in filenames:
                if fn.endswith('.proto'):
                    protos.append(os.path.relpath(os.path.join(root, fn), proto_root))
    else:
        if files:
            protos = [f.strip() for f in files.split(',') if f.strip()]
        else:
            # Search for Server.proto and Messages.proto in all subfolders
            protos = []
            target_files = {'Server.proto', 'Messages.proto'}
            for root, dirs, filenames in os.walk(proto_root):
                for fn in filenames:
                    if fn in target_files:
                        protos.append(os.path.relpath(os.path.join(root, fn), proto_root))
            if not protos:
                print(f"Warning: Could not find Server.proto or Messages.proto in {proto_root}")
                return

    # Try to use the project's virtualenv python if present, otherwise fall back
    venv_python = None
    venv_env = os.environ.get('VIRTUAL_ENV')
    if venv_env:
        venv_path = venv_env
        scripts_dir = 'Scripts' if sys.platform == 'win32' else 'bin'
        python_exe = 'python.exe' if sys.platform == 'win32' else 'python'
        venv_python = os.path.join(venv_path, scripts_dir, python_exe)
    elif os.path.exists(os.path.join(BASE_DIR, '.venv')):
        scripts_dir = 'Scripts' if sys.platform == 'win32' else 'bin'
        python_exe = 'python.exe' if sys.platform == 'win32' else 'python'
        venv_python = os.path.join(BASE_DIR, '.venv', scripts_dir, python_exe)
    python = venv_python or (PYTHON_EXE if 'PYTHON_EXE' in globals() and PYTHON_EXE else 'python')

    # Ensure grpc_tools is available
    check_cmd = f'"{python}" -c "import grpc_tools.protoc; print(1)"'
    try:
        # Use warn=True to catch the return without raising
        res = c.run(check_cmd, hide='both', warn=True)
        if res.exited != 0:
            print('grpc_tools not installed in the selected Python environment. Install with pip install grpcio-tools')
            return
    except Exception:
        print('grpc_tools not installed in the selected Python environment. Install with pip install grpcio-tools')
        return

    args = [python, '-m', 'grpc_tools.protoc', f'-I{proto_root}', f'--python_out={os.path.join(BASE_DIR, "src")}', f'--grpc_python_out={os.path.join(BASE_DIR, "src")}' ]
    args.extend([os.path.join(proto_root, p) for p in protos])
    cmd = ' '.join(f'"{a}"' if ' ' in a else a for a in args)
    print('Running:', cmd)
    c.run(cmd, echo=True, warn=True)
