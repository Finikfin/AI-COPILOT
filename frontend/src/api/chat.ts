import { ENDPOINTS } from '@/constants/api';

export const generatePipeline = async (prompt: string) => {
  try {
    const response = await fetch(ENDPOINTS.PIPELINES.GENERATE, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({ prompt }),
    });
    return response.ok;
  } catch (error) {
    console.error('Error generating pipeline:', error);
    return false;
  }
};
