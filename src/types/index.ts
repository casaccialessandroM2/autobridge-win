export interface InterfaceInfo {
  name:         string;
  ip_addresses: string[];
}

export interface AppConfig {
  relay_url:      string;
  session_id:     string;
  local_bind_ip:  string;
  vin:            string;
  tcp_port:       number;
  udp_port:       number;
}

export interface LogEntry {
  timestamp: string;
  level:     "INFO" | "WARN" | "ERROR" | "DEBUG" | "DOIP";
  message:   string;
}

export type ConnectionStatus = "Disconnected" | "Connecting" | "Connected";
