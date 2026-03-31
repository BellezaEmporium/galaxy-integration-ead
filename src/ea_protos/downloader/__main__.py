from remote_zip import RemoteZip
import requests
import os
import shutil
import zlib
import subprocess

# logging.basicConfig(level=logging.DEBUG)

WHITELISTED_EXES = ["EABackgroundService", "EAConnect_microsoft", "EADesktop", "EAEgsProxy", "EAGEP", "EALaunchHelper", "EALocalHostSvc", "EASteamProxy", "EAUpdater", "Link2EA"]

AUTOPATCH_BUCKET = 'https://autopatch.juno.ea.com/autopatch/upgrade/buckets/999'

def version_url(version: str) -> str:
    return f'https://autopatch.juno.ea.com/autopatch/versions/{version}?locale=en_US'

current_version = open('curr_version.txt', 'r').read().strip()

session = requests.Session()

session.headers['User-Agent'] = 'EADownloader'

bucket_response = session.get(AUTOPATCH_BUCKET)
bucket = bucket_response.json()

recommended_version = bucket['recommended']['version']

if recommended_version == current_version:
    print("Already at the recommended version.")
    exit(0)

version_response = session.get(version_url(recommended_version))
version_manifest = version_response.json()

shutil.rmtree('temp', ignore_errors=True)
os.makedirs('temp')

url = version_manifest['downloadURL']

zip = RemoteZip(url, session)

zip.setup()

for file in zip.central_directory.files:
    file_name = os.path.basename(file.file_name)
    if ".exe" in file.file_name and len(file.file_name.split('/')) == 2 and file_name.split('.')[0] in WHITELISTED_EXES:
        file_data = zip.get_bytes_from_file(file.file_data_offset, file.compressed_size)
        assert file.compression_method == 8
        decompressor = zlib.decompressobj(-15)
        with open(f'temp/{file_name}', 'wb') as f:
            f.write(decompressor.decompress(file_data))

def flatten_proto_files(root_folder: str) -> None:
    for exe in os.listdir(root_folder):
        exe_folder = os.path.join(root_folder, exe)
        if not os.path.isdir(exe_folder):
            continue

        for item in os.listdir(exe_folder):
            item_path = os.path.join(exe_folder, item)

            if os.path.isdir(item_path):
                shutil.rmtree(item_path, ignore_errors=True)
                continue

            if not item.endswith(".proto"):
                os.remove(item_path)


os.makedirs('extracted_protos', exist_ok=True)

for exe in os.listdir('temp'):
    if exe.split('.')[0] in WHITELISTED_EXES:
        file_location = os.path.join('temp', exe)
        subprocess.run(
            ['python', './pbtk/extractors/from_binary.py', file_location, 'extracted_protos/' + exe.split('.')[0]],
            check=True
        )

def move_exe_folders_to_protos(source_root: str, target_root: str) -> None:
    os.makedirs(target_root, exist_ok=True)

    for item in os.listdir(source_root):
        src = os.path.join(source_root, item)
        if not os.path.isdir(src):
            continue

        dst = os.path.join(target_root, item)

        if os.path.exists(dst):
            shutil.rmtree(dst, ignore_errors=True)

        shutil.move(src, dst)

flatten_proto_files("extracted_protos")
move_exe_folders_to_protos("extracted_protos", "../protos")

with open('curr_version.txt', 'w') as f:
    f.write(recommended_version)

shutil.rmtree('temp', ignore_errors=True)
shutil.rmtree('extracted_protos', ignore_errors=True)
