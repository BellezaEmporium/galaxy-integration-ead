import os
import sys
import json
import tempfile
from shutil import rmtree, which
from distutils.dir_util import copy_tree

from invoke.tasks import task
from galaxy.tools import zip_folder_to_file

BASE_DIR = os.path.abspath(os.path.dirname(__file__))

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
        c.run(f'pip-compile requirements/app.txt --output-file=-', out_stream=tmp)

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
