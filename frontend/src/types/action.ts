export interface Action {
  id: string;
  operation_id?: string;
  method: 'GET' | 'POST' | 'PUT' | 'PATCH' | 'DELETE';
  path: string;
  base_url?: string;
  summary?: string;
  description?: string;
  tags?: string[] | null;
  parameters_schema?: unknown;
  request_body_schema?: unknown;
  response_schema?: unknown;
  source_filename?: string;
  raw_spec?: unknown;
  created_at?: string;
  updated_at?: string;
  // For UI compatibility with previous mock data
  tag?: string;
}

export interface Capability {
  id: string;
  user_id?: string | null;
  action_id: string | null;
  type?: 'ATOMIC' | 'COMPOSITE' | string;
  name: string;
  description: string | null;
  input_schema?: Record<string, unknown> | null;
  output_schema?: Record<string, unknown> | null;
  recipe?: CompositeRecipe | null;
  data_format?: Record<string, unknown> | null;
  created_at?: string;
  updated_at?: string;
}

export interface CompositeRecipeStep {
  step: number;
  capability_id: string;
  inputs: Record<string, string>;
}

export interface CompositeRecipe {
  version: 1;
  steps: CompositeRecipeStep[];
}

export interface CreateCompositeCapabilityRequest {
  name: string;
  description?: string | null;
  input_schema?: Record<string, unknown> | null;
  output_schema?: Record<string, unknown> | null;
  recipe: CompositeRecipe;
}

export interface IngestResponse {
  created_count: number;
  actions: Action[];
  succeeded_actions?: Action[];
  capabilities?: Capability[];
  failed_actions?: Array<{
    method?: string;
    path?: string;
    error: string;
  }>;
}
