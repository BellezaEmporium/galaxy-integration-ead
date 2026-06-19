#Requires -Version 5.1
$ErrorActionPreference = "SilentlyContinue"

$jobs = @{
    bios = Start-Job { Get-CimInstance Win32_BIOS        -Property Manufacturer,SerialNumber }
    bb   = Start-Job { Get-CimInstance Win32_BaseBoard   -Property Manufacturer,SerialNumber }
    os   = Start-Job { Get-CimInstance Win32_OperatingSystem -Property SerialNumber,InstallDate }
    gpu  = Start-Job { Get-CimInstance Win32_VideoController -Property PNPDeviceID |
                       Where-Object { $_.PNPDeviceID -match "DEV_[0-9A-F]+" } |
                       Select-Object -First 1 }
    disk = Start-Job { Get-CimInstance Win32_DiskDrive   -Property SerialNumber |
                       Select-Object -First 1 }
    nic  = Start-Job {
               Get-CimInstance Win32_NetworkAdapter `
                   -Filter "PhysicalAdapter=True AND NetEnabled=True" `
                   -Property MACAddress,ServiceName |
               Where-Object {
                   $_.MACAddress -and
                   $_.ServiceName -notmatch "vmnetadapter|vboxnetadp|ndisip|tap|hyperv|loopback"
               } | Select-Object -First 1
           }
}

# Wait for all jobs with a single timeout budget
$null = $jobs.Values | Wait-Job -Timeout 12

$bios = Receive-Job $jobs.bios | Select-Object -First 1
$bb   = Receive-Job $jobs.bb   | Select-Object -First 1
$os   = Receive-Job $jobs.os   | Select-Object -First 1
$gpu  = Receive-Job $jobs.gpu
$disk = Receive-Job $jobs.disk
$nic  = Receive-Job $jobs.nic

$jobs.Values | Remove-Job -Force

$gid = 0
if ($gpu -and $gpu.PNPDeviceID -match "DEV_([0-9A-Fa-f]+)") {
    try { $gid = [Convert]::ToInt32($Matches[1], 16) } catch { $gid = 0 }
}

$mac = ""
if ($nic -and $nic.MACAddress) {
    $clean = $nic.MACAddress -replace "[:\-]", ""
    if ($clean) { $mac = "`$$($clean.ToLower())" }
}

$osi = "1970-01-0100:00:00.000000000+0000"
try {
    $wmiOs = ([wmiclass]"Win32_OperatingSystem").GetInstances() |
             Select-Object -First 1
    if ($wmiOs) { $osi = $wmiOs.InstallDate }
} catch {}


[ordered]@{
    bbm = if ($bios -and $bios.Manufacturer) { $bios.Manufacturer.Trim() } else { "None" }
    bsn = if ($bios -and $bios.SerialNumber) { $bios.SerialNumber.Trim() } else { "None" }
    gid = $gid
    hsn = if ($disk -and $disk.SerialNumber) { $disk.SerialNumber.Trim() } else { "None" }
    mac = $mac
    mbm = if ($bb   -and $bb.Manufacturer)   { $bb.Manufacturer.Trim()   } else { "None" }
    msn = if ($bb   -and $bb.SerialNumber)   { $bb.SerialNumber.Trim()   } else { "None" }
    osi = $osi
    osn = if ($os   -and $os.SerialNumber)   { $os.SerialNumber.Trim()   } else { "None" }
} | ConvertTo-Json -Compress