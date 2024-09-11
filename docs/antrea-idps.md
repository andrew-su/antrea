# Antrea IDPS

## Introduction

Antrea IDPS is an out-of-box solution to integrate Lastline Suricata with Antrea.

## Framework

### IDPS Controller

The Antrea IDPS Controller has the following components:

  - Signature Manager, which is responsible for fetching the latest signature data
    from different signature providers.
  - IDPSPolicy Controller, which is responsible for mirroring an IDPSPolicy object
    to a TrafficControl object.
  - APIServer, which is responsible for exposing the latest signature data fetched 
    by Signature Manager via HTTP APIs. Note that, the authentication and authorization
    of the APIServer are delegated to Kubernetes.
  - Registration Controller, which is responsible for validating the registration of
    Antrea to NSX.

#### Signature Manager

Signature Manager is used to manage multiple signature providers. Currently, we
have only one signature provider NTICS. For every signature provider, it syncs the
latest signature data periodically, and it also provides a unified interface which
is used get the signature data for callers. Note that, the signature data is not
stored in local filesystem. It is only cached in memory.

##### Signature Provider NTICS

NSX Threat Intelligence Cloud (NTICS) exposes some REST APIs. To get the signature
data from NTICS:

  - Use a NSX license to register the current device to NTICS, then a client ID and
    client secret will be returned.
  - Use the client ID and client secret to authenticate to get a token.
  - Use the token to get the signature information, like recent versions, URL to
    download the signature file and checksum of a specified version signature.

For the downloaded signature file, it is compressed in zip. Here is the hierarchy of
files in the file: 

  - IDSSignaturesVersion.txt
  - nsx-ids-bundle.tar.gz.gpg
    - nsx-ids-bundle.tar.gz
      - antimalware-signatures.json
      - antimalware-signatures.rules.gz
      - classification.config
      - ids-signatures.json
      - ids-signatures.tar.gz
        - nsx-idps.addrs.yaml
        - nsx-idps.ports.yaml
        - rules
          - nsx-idps.rules
          - ET_LICENSE_5_0.txt
          - lua
      - changelog.json

The files and directories which are needed by Suricata will be repacked. They are
listed in the following:

  - nsx-ids-bundle.tar.gz/classification.config
  - nsx-ids-bundle.tar.gz/ids-signatures.tar.gz/nsx-idps.addrs.yaml
  - nsx-ids-bundle.tar.gz/ids-signatures.tar.gz/nsx-idps.ports.yaml
  - nsx-ids-bundle.tar.gz/ids-signatures.tar.gz/rules/nsx-idps.rules
  - nsx-ids-bundle.tar.gz/ids-signatures.tar.gz/rules/lua

After downloading and repacking the signature data successfully, corresponding CRD
like the following will be created or updated.

```yaml
apiVersion: crd.antrea.tanzu.vmware.com/v1alpha1
kind: IDPSSignatureProviderInfo
metadata:
  name: ntics
  labels:
    app: antrea-idps
signatureBundle:
  version: 1207
  sha256CheckSum: 79cf63c1659cc7834d652ee794e973da05ba6b07a34402e6885a43fba2d5ce44
```

The struct `signatureBundle` of the CRD is used to store the version number of the
signature and the sha256 checksum of the repacked signature data.

#### IDPSPolicy Controller

IDPSPolicy is a convenient CRD to select Pods with AppliedTo and apply the selected
Pods to an IDS or IPS engine. Its functionality is implemented by IDPSPolicy
Controller. When creating a IDPSPolicy, a corresponding TrafficControl will be
also created. For example, create the following IDPSPolicy:

```yaml
apiVersion: crd.antrea.tanzu.vmware.com/v1alpha1
kind: IDPSPolicy
metadata:
  name: test-idpspolicy
spec:
  appliedTo:
    podSelector:
      matchLabels:
        app: web
```

A TrafficControl will be created like the following:

```yaml
apiVersion: crd.antrea.io/v1alpha2
kind: TrafficControl
metadata:
  name: test-idpspolicy
  annotations:
    crd.antrea.tanzu.vmware.com/managed-by: IDPSPolicyController
spec:
  appliedTo:
    podSelector:
      matchLabels:
        app: nginx
  direction: Both
  action: Mirror
  targetPort:
    device:
      name: antrea-tap0
```

#### APIServer

APIServer is responsible for exposing the latest signature data in Signature Manager
via HTTP API. The signature data of every signature provider is exposed by the URI
`/signatures/<signature provider name>`.

#### Registration Controller

Registration Controller is used by signature provider NTICS to verify the registration
to NSX. The controller watches a CRD NSXRegistration which contains an encrypted
timestamp. The registration is considered valid if the timestamp is decrypted
successfully and the timestamp is not 10 minutes earlier than from the current time.

### IDPS Agent

The Antrea IDPS Agent has the following components:

  - Signature Controller, which is responsible for syncing the latest signature data
    from the APIServer in IDPS Controller.
  - Suricata, which is the IDS engine.

#### Signature Controller

Signature Controller is used to watch CRD IDPSSignatureProviderInfo and sync the
signature data from the APIServer in IDPS Controller. After syncing signature data,
Suricata will be reloaded to load the signature data.

#### Suricata

Suricata is the IDS engine.

## Prerequisites

 - Since Antrea 1.7.0, and enable feature gate TrafficControl in Antrea Agent.
 - Antrea has been registered to NSX.

## Limitations

 - Only IDS mode is supported.
 - Rotation of Suricata alert event log file is only based on timestamp.

## Practical Steps

### Step 1: Deploy Antrea and Antrea-interworking

To deploy Antrea and Antrea-interworking, use:

```bash
ANTREA_BUILD="ob-xxxxx"
ANTREA_INTERWORKING_BUILD="ob-xxxxx"
CLUSTER_NAME="cluter01"
NSX_MANAGER_IP="1.1.1.1"
NSX_MANAGER_USER="dummyUser"
NSX_MANAGER_PASSWORD="dummyPasswd"
DEPLOY_TYPE="k8s-docker" # k8s-docker or k8s-containerd

wget http://build-squid.vcfd.broadcom.net/build/mts/release/bora-$(echo "${ANTREA_IDPS_BUILD}" | cut -d - -f 2)/publish/antrea-interworking/scripts/deploy.sh
./deploy.sh ${ANTREA_BUILD} ${ANTREA_INTERWORKING_BUILD} ${CLUSTER_NAME} ${NSX_MANAGER_IP} ${NSX_MANAGER_USER} ${NSX_MANAGER_PASSWORD} ${DEPLOY_TYPE}
rm -rf ./deploy.sh
```

Note that, please replace the dummy values in above script according to your environment. For more details, please refer
to repo [antrea-interworking](https://gitlab-vmw.devops.broadcom.net/core-build/antrea-interworking).

### Step 2: Deploy Antrea IDPS

To deploy Antrea IDPS, use:

```bash
NSX_LICENSE="00000-00000-00000-00000-00000"
ANTREA_IDPS_BUILD="ob-xxxxx"
DEPLOY_TYPE="k8s-docker" # k8s-docker or k8s-containerd

wget http://build-squid.vcfd.broadcom.net/build/mts/release/bora-$(echo "${ANTREA_IDPS_BUILD}" | cut -d - -f 2)/publish/antrea-idps/scripts/deploy_idps.sh
./deploy_idps.sh --idps-build ${ANTREA_IDPS_BUILD} --nsx-license ${LICENSE} --deploy-type ${DEPLOY_TYPE}
rm -rf ./deploy_idps.sh
```

Note that, please replace the dummy values in above script according to your environment.

### Step 3: Restart Antrea-interworking

After Antrea IDPS is deployed, restart Antrea-interworking, use:

```bash
kubectl rollout restart deployment interworking -n vmware-system-antrea
```

This is because the CRD called NSXRegistration that is used by both Antrea IDPS and Antrea-interworking is defined in
Antrea IDPS deployment yaml file. Antrea-interworking checks whether the NSXRegistration CRD exists only during the Pod
startup, as a result, the Antrea-interworking Pod should be restarted after Antrea IDPS is deployed.

### Testing

To test the IDS functionality, you can create a IDPSPolicy first, use:

```bash
cat <<EOF | kubectl apply -f -
apiVersion: crd.antrea.tanzu.vmware.com/v1alpha1
kind: IDPSPolicy
metadata:
  name: test-ids-policy
spec:
  appliedTo:
    podSelector:
      matchLabels:
        app: web
EOF
```

Then create a Pod with the `app=web` label, using the following command:

```bash
kubectl create deploy web --image nginx:1.21.6
```

Let's log into the Node that the test Pod runs on and start `tail` to see
updates to the alert log. For example, the log file is
`/var/log/antrea/suricata/eve.alert.2022-08-15.json`. Note that,
the alert log is rotated every day, as a result, the generic filename of
alert log is `eve.alert.%Y-%m-%d.json`.

```bash
tail -f /var/log/antrea/suricata/eve.alert.2022-08-15.json
```

you can `kubectl exec` into the Pods and generate malicious requests against
external web server with the following command:

```bash
kubectl exec deploy/web -- curl -s http://testmynids.org/uid/index.html
```

The following output should now be seen in the alert log:

```json
{"timestamp":"2022-08-15T01:48:40.650786+0000","flow_id":425565324104703,"in_iface":"suricata-tap0","event_type":"alert","src_ip":"99.84.238.95","src_port":80,"dest_ip":"10.10.0.59","dest_port":38622,"proto":"TCP","direction":"to_client","metadata":{"flowbits":["LL.priority_fb","LL.105241_0"],"flowints":{"client.idx":1,"server.idx":1}},"tx_id":0,"alert":{"action":"allowed","gid":1,"signature_id":4101454,"rev":3,"signature":"SLR Alert - TESTING IDPS ALERT SYSTEM - TESTMYIDS.COM","category":"Unknown Classtype","severity":4,"source":{"ip":"99.84.238.95","port":80},"target":{"ip":"10.10.0.59","port":38622},"metadata":{"blacklist_mode":["DISABLED"],"confidence":["90"],"created_at":["2019_01_01","2019_01_01"],"detector_id":["101357"],"exploited":["None"],"flip_endpoints":["True"],"ids_mode":["REAL"],"lock":["false"],"policy":["suricata-ips","suricata-ids","security-ips drop","balanced-ips drop","connectivity-ips drop"],"server_side":["False"],"severity":["1"],"signature_severity":["Major"],"threat_class_name":["Functional Testing"],"threat_name":["IDS Test testmyids.com"],"type":["suricata"]}},"http":{"hostname":"testmynids.org","url":"/uid/index.html","http_user_agent":"curl/7.74.0","http_content_type":"text/html","http_method":"GET","protocol":"HTTP/1.1","status":200,"length":39,"direction":"download"},"files":[{"filename":"/uid/index.html","sid":[],"gaps":false,"state":"CLOSED","stored":false,"size":39,"tx_id":0}],"app_proto":"http","flow":{"pkts_toserver":3,"pkts_toclient":3,"bytes_toserver":298,"bytes_toclient":743,"start":"2022-08-15T01:48:40.644095+0000"}}
```

## Alert Logs Collection

Please refer to [docs/cookbook/fluentd/README.md](./cookbooks/fluentd/README.md).
Note that, in `Step 3: Configure Custom Fluentd Plugins`of the document,
replace the configuration file `kubernetes.conf` with the following:

```editorconfig
<match fluent.**>
  @type null
</match>

<source>
  @type tail
  read_from_head true
  path /var/log/antrea/suricata/sockets/eve.alert.*
  pos_file /var/log/fluentd-suricata.pos
  tag suricata
  <parse>
    @type json
    time_type string
    time_format %Y-%m-%dT%H:%M:%S.%6N%z
  </parse>
</source>

<filter kubernetes.**>
  @type kubernetes_metadata
  @id filter_kube_metadata
  kubernetes_url "#{ENV['FLUENT_FILTER_KUBERNETES_URL'] || 'https://' + ENV.fetch('KUBERNETES_SERVICE_HOST') + ':' + ENV.fetch('KUBERNETES_SERVICE_PORT') + '/api'}"
  verify_ssl "#{ENV['KUBERNETES_VERIFY_SSL'] || true}"
  ca_file "#{ENV['KUBERNETES_CA_FILE']}"
</filter>
```