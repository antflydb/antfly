import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { useSearchParams } from "react-router-dom";
import { useApiConfig } from "@/hooks/use-api-config";

interface ModelSelectionOptions {
  models: string[];
  defaultModel?: string;
  preserveRequestedModel?: boolean;
}

/** Keeps playground model and connection identity stable in shareable deep links. */
export function useModelSelection({
  models,
  defaultModel,
  preserveRequestedModel = false,
}: ModelSelectionOptions) {
  const [searchParams, setSearchParams] = useSearchParams();
  const { inferenceConnectionId, setInferenceConnectionId } = useApiConfig();
  const requestedModel = searchParams.get("model") ?? undefined;
  const requestedConnection = searchParams.get("connection") ?? undefined;
  const appliedRequestedConnectionRef = useRef<string | undefined>(undefined);
  const applyingConnectionRef = useRef<string | undefined>(undefined);
  const initial =
    (requestedModel && (preserveRequestedModel || models.includes(requestedModel))
      ? requestedModel
      : undefined) ??
    (defaultModel && models.includes(defaultModel) ? defaultModel : undefined) ??
    models[0] ??
    "";
  const [model, setModelState] = useState(initial);

  useEffect(() => {
    if (!requestedConnection || appliedRequestedConnectionRef.current === requestedConnection)
      return;
    appliedRequestedConnectionRef.current = requestedConnection;
    if (requestedConnection === inferenceConnectionId) return;
    applyingConnectionRef.current = requestedConnection;
    setInferenceConnectionId(requestedConnection);
  }, [inferenceConnectionId, requestedConnection, setInferenceConnectionId]);

  useEffect(() => {
    if (applyingConnectionRef.current) {
      if (applyingConnectionRef.current === inferenceConnectionId) {
        applyingConnectionRef.current = undefined;
      }
      return;
    }
    if (requestedConnection === inferenceConnectionId) return;
    setSearchParams(
      (current) => {
        const next = new URLSearchParams(current);
        next.set("connection", inferenceConnectionId);
        return next;
      },
      { replace: true }
    );
  }, [inferenceConnectionId, requestedConnection, setSearchParams]);

  useEffect(() => {
    // Connection deep links are applied in the effect above. Until that state
    // update reaches this hook, `models` still belongs to the previous
    // connection. Reconciling against it would replace a valid model deep link
    // and write the previous connection back into the URL.
    if (requestedConnection && requestedConnection !== inferenceConnectionId) return;

    if (requestedModel && (preserveRequestedModel || models.includes(requestedModel))) {
      setModelState(requestedModel);
      return;
    }

    // An empty inventory commonly means that discovery is still in flight. Keep
    // the requested deep link intact until we can determine whether it is stale.
    if (models.length === 0) {
      if (model) setModelState("");
      return;
    }

    const modelIsAvailable = models.includes(model);
    if (!modelIsAvailable || requestedModel) {
      const fallback = modelIsAvailable
        ? model
        : ((defaultModel && models.includes(defaultModel) ? defaultModel : models[0]) ?? "");
      if (fallback !== model) setModelState(fallback);
      setSearchParams(
        (current) => {
          const next = new URLSearchParams(current);
          if (fallback) next.set("model", fallback);
          else next.delete("model");
          next.set("connection", inferenceConnectionId);
          return next;
        },
        { replace: true }
      );
    }
  }, [
    defaultModel,
    inferenceConnectionId,
    model,
    models,
    preserveRequestedModel,
    requestedConnection,
    requestedModel,
    setSearchParams,
  ]);

  const setModel = useCallback(
    (nextModel: string) => {
      setModelState(nextModel);
      setSearchParams(
        (current) => {
          const next = new URLSearchParams(current);
          if (nextModel) next.set("model", nextModel);
          else next.delete("model");
          next.set("connection", inferenceConnectionId);
          return next;
        },
        { replace: true }
      );
    },
    [inferenceConnectionId, setSearchParams]
  );

  return useMemo(
    () => ({ model, setModel, connectionId: inferenceConnectionId }),
    [inferenceConnectionId, model, setModel]
  );
}
