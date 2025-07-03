import React, { createContext, useState, useEffect } from "react";

interface AuthCtx { token: string|null; setToken:(t:string|null)=>void; }
export const AuthContext = createContext<AuthCtx>({token:null,setToken:()=>{}});

export const AuthProvider:React.FC<{children:React.ReactNode}> = ({children})=>{
  const [token,setToken]=useState<string|null>(()=>localStorage.getItem("token"));
  useEffect(()=>{ token?localStorage.setItem("token",token):localStorage.removeItem("token") },[token]);
  return <AuthContext.Provider value={{token,setToken}}>{children}</AuthContext.Provider>;
}
