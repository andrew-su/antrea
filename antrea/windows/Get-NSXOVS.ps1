<#
.SYNOPSIS
    This script downloads NSX OVS and converts directory organization to a new zip file.
    The target organization is same with the upstream OVS package used by upstream Antrea.

.DESCRIPTION
    This script downloads NSX OVS and converts directory organization to a new zip file.
    The target organization is same with the upstream OVS package used by upstream Antrea.

.PARAMETER DownloadDir
    Specifies the path to download NSX OVS and Microsoft Visual C++ Redistributable packages.

.PARAMETER OVSUrl
    Specifies the download URL of NSX OVS.

.PARAMETER VCRedistUrl
    Specifies the download URL of Microsoft Visual C++ Redistributable packages.

.PARAMETER OutPutFile
    Specifies the output file path.

.PARAMETER ExtractCertificate
    Specifies if extract certificate from driver file.

.EXAMPLE
    PS C:\> $NSXOVSUrl="http://build-squid.vcfd.broadcom.net/build/mts/release/bora-17654316/publish/windows_x64/openvswitch_2.13.1.17654316-win64.zip"
    PS C:\> $VCRedistUrl="http://packages.vcfd.broadcom.net/artifactory/nsbu-windows-local/vcredists.zip"
    PS C:\> .\Get-NSXOVS.ps1 -OVSUrl $NSXOVSUrl -VCRedistUrl $VCRedistUrl

    The outout OVS package will be ./ovs-win64.zip

#>
Param(
    [parameter(Mandatory = $false)] [string] $DownloadDir,
    [parameter(Mandatory = $true)] [string] $OVSUrl,
    [parameter(Mandatory = $true)] [string] $VCRedistUrl,
    [parameter(Mandatory = $false)] [string] $OutPutFile = "ovs-win64.zip",
    [parameter(Mandatory = $false)] [boolean] $ExtractCertificate=$false
)
$ErrorActionPreference = "Stop"

function Log($Info) {
    $time = $(get-date -Format g)
    "$time $Info `n`r" | Tee-Object $InstallLog -Append | Write-Host
}

if (!$DownloadDir) {
    $DownloadDir = [System.IO.Path]::GetDirectoryName($myInvocation.MyCommand.Definition)
}

$TempDir = Join-Path -Path $DownloadDir -ChildPath "nsx-ovs-temp"

$InstallLog = "$DownloadDir\install_ovs.log"
$OVSZip = "$DownloadDir\nsx-ovs.zip"
$VCRedistZip = "$DownloadDir\vcredists.zip"

if (Test-Path -Path $OutPutFile) {
    Log "File $OutPutFile already exists"
    return
}

# Download nsx-ovs zip
if (!(Test-Path -Path $DownloadDir)) {
    mkdir -p $DownloadDir
}
if (!(Test-Path -Path $TempDir)) {
    mkdir -p $TempDir
}
Log "Using temp dir: $TempDir"
if (Test-Path -Path $OVSZip) {
    Log "File: $OVSZip already exists"
} else {
    Log "Downloading OVS package from $OVSUrl to $OVSZip"
    curl.exe -sLo $OVSZip $OVSUrl
}

if (Test-Path -Path $VCRedistZip) {
    Log "File: $VCRedistZip already exists"
} else {
    Log "Downloading Microsoft Visual C++ Redistributable package from $VCRedistUrl to $VCRedistZip"
    curl.exe -sLo $VCRedistZip $VCRedistUrl
}


# Expand nsx-ovs and vcredists zip
Log "Extracting $OVSZip to $DownloadDir"
Expand-Archive -Path $OVSZip -DestinationPath $TempDir | Out-Null
Log "Extracting $VCRedistZip to $DownloadDir"
Expand-Archive -Path $VCRedistZip -DestinationPath $TempDir | Out-Null

# Reorganize OVS directory to keep the dir structure same with upstream OVS
$OVSDir = "$TempDir\openvswitch"
$OVSDriverDir = "$OVSDir\driver"
$VCRedistDir = "$OVSDir\redist"
Move-Item $TempDir\include $OVSDir
Move-Item $TempDir\lib $OVSDir
Move-Item $TempDir\scripts $OVSDir
Move-Item $TempDir\vcredist2017 $VCRedistDir
Move-Item $TempDir\ovsext\win10_x64 $OVSDriverDir


# Extract driver certificate
$CertificateFile="$OVSDriverDir\package.cer"
if ($ExtractCertificate -and !(Test-Path -Path $CertificateFile)) {
    $DriverFile="$OVSDriverDir\OVSExt.sys"
    $ExportType = [System.Security.Cryptography.X509Certificates.X509ContentType]::Cert;
    $Cert = (Get-AuthenticodeSignature $DriverFile).SignerCertificate;
    [System.IO.File]::WriteAllBytes($CertificateFile, $Cert.Export($ExportType));
}

# Generate new OVS zip
Log "Generating new OVS zip file: $OutPutFile"
Compress-Archive -Path "$TempDir\openvswitch" -DestinationPath $OutPutFile
Remove-Item -Recurse "$TempDir"
