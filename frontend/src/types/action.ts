export interface Action {
  id: string;
  operation_id?: string;
  method: 'GET' | 'POST' | 'PUT' | 'PATCH' | 'DELETE';
  path: string;
  base_url?: string;
  summary?: string;
  description?: string;
  tags?: string[] | null;
  parameters_schema?: any;
  request_body_schema?: any;
  response_schema?: any;
  source_filename?: string;
  raw_spec?: any;
  created_at?: string;
  updated_at?: string;
  // For UI compatibility with previous mock data
  tag?: string;
}

export interface IngestResponse {
  created_count: number;
  actions: Action[];
  succeeded_actions?: Action[];
  failed_actions?: Array<{
    method?: string;
    path?: string;
    error: string;
  }>;
}
