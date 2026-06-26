export interface InterfaceInfo {
  name:         string;
  ip_addresses: string[];
}

export interface AppConfig {
  mac_ip:        string;
  mac_ws_port:   number;
  local_bind_ip: string;
  vin:           string;
  session_label: string;
}

export interface LogEntry {
  timestamp: string;
  level:     "INFO" | "WARN" | "ERROR" | "DEBUG" | "DOIP";
  message:   string;
}

export type ConnectionStatus = "Disconnected" | "Connecting" | "Connected";
