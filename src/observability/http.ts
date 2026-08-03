export interface HttpResponseData {
  status: number;
  bodyText: string;
}

export interface HttpPostRequest {
  url: string;
  headers: Record<string, string>;
  body: string;
}

export type HttpPost = (request: HttpPostRequest, signal: AbortSignal) => Promise<HttpResponseData>;

export const defaultHttpPost: HttpPost = async (request, signal) => {
  const response = await fetch(request.url, {
    method: "POST",
    headers: request.headers,
    body: request.body,
    signal
  });

  const bodyText = await response.text();
  return {
    status: response.status,
    bodyText
  };
};

export function isTransientStatus(status: number): boolean {
  return status === 408 || status === 409 || status === 425 || status === 429 || status >= 500;
}
