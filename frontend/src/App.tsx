/**
 * - Tooltip provider
 * 
 * @author Krok Development Team
 * @version 1.0.0
 */

import { Toaster } from "sonner";
import { TooltipProvider } from "@/components/ui/tooltip";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { BrowserRouter, Routes, Route, Navigate } from "react-router-dom";
import { AuthProvider } from "@/contexts/AuthContext";
import { ActionProvider } from "@/contexts/ActionContext";
import { PipelineProvider } from "@/contexts/PipelineContext";
import { Layout } from "@/components/layout/Layout";
import { ProtectedRoute } from "@/components/shared/ProtectedRoute";
import Actions from "./pages/Actions";
import Home from "./pages/Home";
import Capabilities from "./pages/Capabilities";
import Pipelines from "./pages/Pipelines";
import NotFound from "./pages/NotFound";
import Login from "./pages/Login";
import Register from "./pages/Register";

/**
 * QueryClient instance for managing server state
 */
const queryClient = new QueryClient();

/**
 * AppRoutes component
 * 
 * Defines the routing structure for the application.
 */
const AppRoutes = () => {
  return (
    <Routes>
      {/* Public Routes */}
      <Route path="/login" element={<Login />} />
      <Route path="/register" element={<Register />} />

      {/* Protected Main Application Routes */}
      <Route element={<ProtectedRoute />}>
        <Route path="/" element={<Layout />}>
          <Route index element={<Home />} />
          <Route path="actions" element={<Actions />} />
          <Route path="capabilities" element={<Capabilities />} />
          <Route path="pipelines" element={<Pipelines />} />
        </Route>
      </Route>

      {/* 404 page for unmatched routes */}
      <Route path="*" element={<NotFound />} />
    </Routes>
  );
};

/**
 * Main App component
 * 
 * Root component that wraps the entire application with necessary providers:
 * - QueryClientProvider: For data fetching and caching
 * - AuthProvider: For authentication state management
 * - TooltipProvider: For tooltip functionality
 * - Toaster: For toast notifications
 * - BrowserRouter: For client-side routing
 * 
 * @returns JSX.Element - The complete application structure
 */
const App = () => (
  <QueryClientProvider client={queryClient}>
    <AuthProvider>
      <TooltipProvider>
        <ActionProvider>
          <PipelineProvider>
            {/* Toast notification system configuration */}
            <Toaster
              position="top-right"
              theme="dark"
              duration={3500}
              toastOptions={{
                style: {
                  background: 'hsl(var(--card))',
                  color: 'hsl(var(--foreground))',
                  border: '1px solid hsl(var(--border))',
                  boxShadow: '0 8px 32px 0 rgba(0, 0, 0, 0.3)',
                }
              }}
            />
            {/* Router with basename for deployment path */}
            <BrowserRouter basename="/">
              <AppRoutes />
            </BrowserRouter>
          </PipelineProvider>
        </ActionProvider>
      </TooltipProvider>
    </AuthProvider>
  </QueryClientProvider>
);

export default App;
