/**
 * @fileoverview Main entry point for AI Copilot MVP application
 * 
 * This file serves as the application entry point, initializing the React
 * application and mounting it to the DOM. It imports the main App component
 * and global styles.
 * 
 * The application is rendered into the DOM element with id "root" and
 * provides the foundation for the entire AI Copilot MVP application.
 * 
 * @author AI Copilot Development Team
 * @version 1.0.0
 */

import { createRoot } from 'react-dom/client'
import App from './App.tsx'
import './index.css'
import { AppErrorBoundary } from '@/components/shared/AppErrorBoundary'

window.addEventListener('error', (event) => {
	console.error('Global window error', event.error || event.message)
})

window.addEventListener('unhandledrejection', (event) => {
	console.error('Unhandled promise rejection', event.reason)
})

/**
 * Initialize and render the React application
 * 
 * Creates a React root and renders the main App component into the DOM.
 * The application is mounted to the element with id "root".
 */
createRoot(document.getElementById("root")!).render(
	<AppErrorBoundary>
		<App />
	</AppErrorBoundary>
);
