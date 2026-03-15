import { type ReactNode, useReducer } from "react";
import { type SharedAction, SharedContext, type SharedState } from "./SharedContext";

export interface SharedContextProviderProps {
  reducer: (state: SharedState, action: SharedAction) => SharedState;
  initialState: SharedState;
  children: ReactNode;
}

export const SharedContextProvider = ({
  reducer,
  initialState,
  children,
}: SharedContextProviderProps) => {
  return (
    <SharedContext.Provider value={useReducer(reducer, initialState)}>
      {children}
    </SharedContext.Provider>
  );
};
