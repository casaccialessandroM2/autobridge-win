export interface AppConfig {
  mac_ip:          string;
  mac_ws_port:     number;
  local_doip_port: number;
  session_label:   string;
}

export interface LogEntry {
  timestamp: string;
  level:     "INFO" | "WARN" | "ERROR" | "DEBUG" | "DOIP";
  message:   string;
}

export type ConnectionStatus = "Disconnected" | "Connecting" | "Connected";
