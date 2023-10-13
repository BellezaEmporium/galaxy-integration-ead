import subprocess
import platform
import os
import time
import tempfile
if platform.system() == "Darwin":
    import psutil
from hashlib import sha1, sha3_256
import aes

# If the file already was decrypted before, we do not need to decrypt it again.
if os.path.exists(os.path.join(tempfile.gettempdir(), "is.json")):
    print("IS already decrypted. Skipping.")
    exit()
else:
    # changes in overall functioning (as of its release) constraints us to use a decryption method in order to check which game's installed
    # inputs the encrypted manifest file
    # outputs the decrypted JSON file
    if platform.system() == "Windows":
        file_path = os.path.join(os.environ.get("ProgramData", os.environ.get("SystemDrive", "C:") + R"\ProgramData"), "EA Desktop", "530c11479fe252fc5aabc24935b9776d4900eb3ba58fdc271e0d6229413ad40e", "IS")
    elif platform.system() == "Darwin":
        file_path = os.path.join(os.sep, "Library", "Application Support", "EA Desktop", "530c11479fe252fc5aabc24935b9776d4900eb3ba58fdc271e0d6229413ad40e", "IS")
    else:
        file_path = "IS"
    iv = "allUsersGenericIdIS".encode('ascii')
    iv_hash = sha3_256(iv).digest()

    if platform.system() == "Windows":
        query_baseboard = subprocess.check_output('wmic baseboard get Manufacturer,SerialNumber /format:list', shell=True)
        if query_baseboard:
            data = {}
            for line in query_baseboard.decode('utf-8').split('\n'):
                if '=' in line:
                    key, value = line.split('=', 1)
                    data[key] = value.rstrip('\r')
            baseboard_manufacturer = data.get('Manufacturer')
            baseboard_serial_number = data.get('SerialNumber')
        query_bios = subprocess.check_output('wmic bios get Manufacturer,SerialNumber /format:list')
        if query_bios:
            data = {}
            for line in query_bios.decode('utf-8').split('\n'):
                if '=' in line:
                    key, value = line.split('=', 1)
                    data[key] = value.rstrip('\r')
            bios_manufacturer, bios_serial_number = data.get('Manufacturer'), data.get('SerialNumber')
        query_logicaldisk = subprocess.check_output('wmic logicaldisk where "Caption=\'C:\'" get VolumeSerialNumber /format:list')
        if query_logicaldisk:
            data = {}
            for line in query_logicaldisk.decode('utf-8').split('\n'):
                if '=' in line:
                    key, value = line.split('=', 1)
                    data[key] = value.rstrip('\r')
            volume_serial_number = data.get('VolumeSerialNumber')
        query_videocontroller = subprocess.check_output('wmic path win32_videocontroller get PNPDeviceID /format:list')
        if query_videocontroller:
            data = {}
            for line in query_videocontroller.decode('utf-8').split('\n'):
                if '=' in line:
                    key, value = line.split('=', 1)
                    data[key] = value.rstrip('\r')
            video_controller_pnp_device_id = data.get('PNPDeviceID')[0].replace("&amp;", "&")
        query_cpu = subprocess.check_output('wmic cpu get Manufacturer,ProcessorId /format:list')
        if query_cpu:
            data = {}
            for line in query_cpu.decode('utf-8').split('\n'):
                if '=' in line:
                    key, value = line.split('=', 1)
                    data[key] = value.rstrip('\r')
            processor_manufacturer, processor_id = data.get('Manufacturer'), data.get('ProcessorId')
        query_cpu_name = subprocess.check_output('wmic cpu get Name /format:list')
        if query_cpu_name:
            data = {}
            for line in query_cpu_name.decode('utf-8').split('\n'):
                if '=' in line:
                    key, value = line.split('=', 1)
                    data[key] = value.rstrip('\r')
            processor_name = data.get('Name')
        
    elif platform.system() == "Darwin":
        # Retrieve system information
        baseboard_manufacturer = ''
        baseboard_serial_number = ''
        bios_manufacturer = ''
        bios_serial_number = ''
        volume_serial_number = ''
        processor_manufacturer = ''
        processor_id = ''
        processor_name = ''

        # Baseboard information
        baseboard_manufacturer = subprocess.check_output(["system_profiler", "SPHardwareDataType"])
        baseboard_manufacturer = baseboard_manufacturer.decode('utf-8').split(':')[-1].strip()

        # BIOS information (Mac doesn't have BIOS in the traditional sense)
        bios_manufacturer = "Apple"
        bios_serial_number = subprocess.check_output(["system_profiler", "SPHardwareDataType"])
        bios_serial_number = bios_serial_number.decode('utf-8').split(':')[-1].strip()

        # Disk information
        volume_serial_number = subprocess.check_output(["system_profiler", "SPStorageDataType"])
        volume_serial_number = volume_serial_number.decode('utf-8').split('Serial Number (system):')[-1].strip()

        # Video controller information
        # to be determined

        # Processor information
        processor_info = subprocess.check_output(["sysctl", "-n", "machdep.cpu.brand_string"])
        processor_info = processor_info.decode('utf-8').strip()

        processor_manufacturer = "Intel"  # Assuming Mac uses Intel processors
        processor_name = processor_info
        
    elif platform.system() == "Linux":
        # Retrieve system information
        baseboard_manufacturer = ''
        baseboard_serial_number = ''
        bios_manufacturer = ''
        bios_serial_number = ''
        volume_serial_number = ''
        video_controller_pnp_device_id = ''
        processor_manufacturer = ''
        processor_id = ''
        processor_name = ''

        # Baseboard information
        with open('/sys/class/dmi/id/board_vendor', 'r') as f:
            baseboard_manufacturer = f.read().strip()

        with open('/sys/class/dmi/id/board_serial', 'r') as f:
            baseboard_serial_number = f.read().strip()

        # BIOS information
        with open('/sys/class/dmi/id/bios_vendor', 'r') as f:
            bios_manufacturer = f.read().strip()

        with open('/sys/class/dmi/id/bios_version', 'r') as f:
            bios_serial_number = f.read().strip()

        # Disk information (assuming the root partition is mounted at /)
        partition = psutil.disk_partitions(all=False)[0]
        volume_serial_number = psutil.disk_usage(partition.mountpoint).serial

        # Video controller information (you may need to adjust this)
        for device in psutil.pids():
            try:
                cmdline = psutil.Process(device).cmdline()
                if "Xorg" in cmdline or "xorg" in cmdline:
                    # Extract video controller information from Xorg process
                    # TEST THIS
                    video_controller_pnp_device_id = cmdline[cmdline.index("Xorg") + 1]
                    pass
            except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                pass

        # Processor information
        processor_info = os.popen("lscpu").read()
        lines = processor_info.split('\n')
        for line in lines:
            if "Vendor ID:" in line:
                processor_manufacturer = line.split(':')[1].strip()
            elif "Model name:" in line:
                processor_name = line.split(':')[1].strip()

    # sha1 string
    hw_info = baseboard_manufacturer + ";" + baseboard_serial_number + ";" + bios_manufacturer + ";" + bios_serial_number + ";" + volume_serial_number + ";" + video_controller_pnp_device_id + ";" + processor_manufacturer + ";" + processor_id + ";" + processor_name + ";"
    print(f"Got hardware info: {hw_info}")

    # Calculate SHA1 Hash of hardware string
    hw_info_bytes = hw_info.encode('ascii')
    hw_hash = sha1(hw_info_bytes).digest()

    print(f"Got hardware hash: {hw_hash.hex()}")

    hash_str = 'allUsersGenericIdIS' + hw_hash.hex().lower()

    # Calculate SHA3 256 Hash of full string
    hash_bytes = hash_str.encode('ascii')
    key_hash = sha3_256(hash_bytes).digest()


    print(f"Got key hash: {key_hash.hex()}")

    aes = aes.AESModeOfOperationCBC(key_hash, iv_hash[0:16])

    # Open input and output files
    with open(file_path, 'rb') as infile, open(os.path.join(tempfile.gettempdir(), 'is.json'), 'wb') as outfile:
        # Read the first 64 bytes and discard them
        infile.read(64)
        block_size = 16

        while True:
            # Read a block from the input file
            block = infile.read(block_size)
            if not block:
                break  # Reached end of file

            # Decrypt the block and write to the output file
            decrypted_block = aes.decrypt(block)
            outfile.write(decrypted_block)

    # verifying the JSON part
    # seems like there's undescribed characters ( or ) in the end of the json file
    # so we need to remove them

    json_string = ""
    with open(os.path.join(tempfile.gettempdir(), "is.json"), "r") as f:
        json_string = f.read().replace("", "").replace("", "")
    with open(os.path.join(tempfile.gettempdir(), "is.json"), "w") as f:
        f.write(json_string)

    print("IS decrypted successfully.")

time.sleep(5)