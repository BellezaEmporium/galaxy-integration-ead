try {
    $ErrorActionPreference = "SilentlyContinue"

    $cimCmdlet = "Get-CimInstance"

    function Get-CIPropertyValue {
        param($ciObj, [string[]]$candidates)
        foreach ($name in $candidates) {
            if ($ciObj -and $ciObj.PSObject.Properties.Name -contains $name) {
                $val = $ciObj.$name
                if ($null -ne $val -and $val -ne '') { return $val.ToString() }
            }
        }
        return ""
    }

    $bios = & $cimCmdlet Win32_BIOS | Select-Object -First 1
    $biosMan = if ($bios -and $bios.Manufacturer) { $bios.Manufacturer.ToString() } else { "" }
    $biosSerial = if ($bios -and $bios.SerialNumber) { $bios.SerialNumber.ToString() } else { "" }

    $bb = & $cimCmdlet Win32_BaseBoard | Select-Object -First 1
    $bbMan = if ($bb -and $bb.Manufacturer) { $bb.Manufacturer.ToString() } else { "" }
    $bbSerial = if ($bb -and $bb.SerialNumber) { $bb.SerialNumber.ToString() } else { "" }

    $os = & $cimCmdlet Win32_OperatingSystem | Select-Object -First 1
    $osSerial = if ($os -and $os.SerialNumber) { $os.SerialNumber.ToString() } else { "" }
    $installDate = if ($os -and $os.InstallDate) { $os.InstallDate.ToString() } else { "0" }

    if (-not $bb) {
        $bb = & $cimCmdlet Win32_BaseBoard | Select-Object -First 1
        $bbMan = if ($bb -and $bb.Manufacturer) { $bb.Manufacturer.ToString() } else { "" }
    }

    if (-not $bbSerial -and $bb) {
        $bbSerial = if ($bb.SerialNumber) { $bb.SerialNumber.ToString() } else { "" }
    }

    # Use CIM/WMI for other info (GPU, disk, network)
    $videoControllers = & $cimCmdlet Win32_VideoController | Where-Object { $_.PNPDeviceID -match "DEV_[0-9A-F]+" } | Select-Object -First 1
    $diskDrive = & $cimCmdlet Win32_DiskDrive | Select-Object -First 1
    $networkAdapter = & $cimCmdlet Win32_NetworkAdapter | Where-Object { $_.PhysicalAdapter -eq $true -and $_.NetEnabled -eq $true -and $_.ServiceName -notmatch "vmnetadapter|vboxnetadp|ndisip|tap|hyperv|loopback" -and $_.MACAddress } | Select-Object -First 1
    # EA uses a specific WMI object for the OS Install Date, if it's not written like shown, it falsifies the mid.
    $os = Get-WmiObject -Class Win32_OperatingSystem
    $installDate = $os.InstallDate
    # Extract GPU device ID (hex -> int)
    $gid = 0
    if ($videoControllers -and $videoControllers.PNPDeviceID) {
        if ($videoControllers.PNPDeviceID -match "DEV_([0-9A-F]+)") {
            try { $gid = [Convert]::ToInt32($matches[1], 16) } catch { $gid = 0 }
        }
    }

    # Format MAC as before
    $mac = ""
    if ($networkAdapter -and $networkAdapter.MACAddress) {
        $macClean = $networkAdapter.MACAddress -replace "[:-]", ""
        if ($macClean) { $mac = "`$" + $macClean.ToLower() }
    }

    $result = @{
        bbm = $biosMan
        bsn = $biosSerial
        gid = $gid
        hsn = if ($diskDrive -and $diskDrive.SerialNumber) { $diskDrive.SerialNumber.ToString().Trim() } else { "" }
        mbm = $bbMan
        msn = $bbSerial
        mac = $mac
        osi = $installDate
        osn = $osSerial
    }

    $result | ConvertTo-Json -Compress

} catch {
    $errorResult = @{
        bbm = ""
        bsn = ""
        gid = 0
        hsn = ""
        mbm = ""
        msn = ""
        mac = ""
        osn = ""
        osi = "0"
    }
    $errorResult | ConvertTo-Json -Compress
}