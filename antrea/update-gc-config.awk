BEGIN { modify=0 }

/  antrea-agent.conf: \|/ {
  modify=1
}

/#hostGateway:/ {
  if (modify == 1) {
    gsub("#hostGateway:.*", "hostGateway: antrea-gw0", $0)
  }
}
/#tunnelType:/ {
  if (modify == 1) {
    gsub("#tunnelType:.*", "tunnelType: geneve", $0)
  }
}
/#serviceCIDR:/ {
  if (modify == 1) {
    gsub("#serviceCIDR:.*", "serviceCIDR: {{.ClusterIPCIDR}}", $0)
  }
}
/---/ {
  modify=0
}

{ print $0 }
