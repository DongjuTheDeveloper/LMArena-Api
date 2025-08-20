// ==UserScript==
// @name         LMArena Proxy Injector
// @namespace    https://github.com/zhongruichen/lmarena-proxy
// @version      1.1.0
// @description  A powerful injector script for LMArena reverse proxy, enabling OpenAI-compatible API access.
// @author       zhongruichen
// @match        https://*.lmarena.ai/*
// @icon         https://www.google.com/s2/favicons?sz=64&domain=lmarena.ai
// @grant        none
// @run-at       document-start
// @all-frames   true
// @updateURL    https://raw.githubusercontent.com/zhongruichen/lmarena-proxy/main/lmarena_injector.user.js
// @downloadURL  https://raw.githubusercontent.com/zhongruichen/lmarena-proxy/main/lmarena_injector.user.js
// ==/UserScript==
(function () {
    'use strict';

    // --- CONFIGURATION ---
    // 如果你的代理服务器不在本机运行，请修改此处的 IP 地址。
    const CONFIG = {
        SERVER_URL: "ws://localhost:9080/ws",
    };

    // --- Constants ---
    const TARGET_API_PATH = "/api/stream/create-evaluation";
    const REQUIRED_COOKIE = "arena-auth-prod-v1";

    // --- State ---
    let socket;
    let isRefreshing = false;
    let pendingRequests = [];
    let modelRegistrySent = false;
    let latestTurnstileToken = null;

    // Track active fetch requests for abort capability
    const activeFetchControllers = new Map(); // requestId -> AbortController

    // --- Human-like Click Simulation ---
    function simulateHumanClick() {
        // Get viewport dimensions
        const viewportWidth = window.innerWidth;
        const viewportHeight = window.innerHeight;

        // Calculate center area with some randomness (within 10% of center)
        const centerX = viewportWidth / 2;
        const centerY = viewportHeight / 2;
        const randomOffsetX = (Math.random() - 0.5) * (viewportWidth * 0.1);
        const randomOffsetY = (Math.random() - 0.5) * (viewportHeight * 0.1);

        const clickX = Math.round(centerX + randomOffsetX);
        const clickY = Math.round(centerY + randomOffsetY);

        // Ensure click is within viewport bounds
        const finalX = Math.max(10, Math.min(viewportWidth - 10, clickX));
        const finalY = Math.max(10, Math.min(viewportHeight - 10, clickY));

        console.log(`[Auth] 🖱️ Simulating human-like click at (${finalX}, ${finalY})`);

        // Create and dispatch mouse events to simulate human interaction
        const target = document.elementFromPoint(finalX, finalY) || document.body;

        // Simulate mouse down, up, and click with slight delays
        const mouseDown = new MouseEvent('mousedown', {
            bubbles: true,
            cancelable: true,
            clientX: finalX,
            clientY: finalY,
            button: 0
        });

        const mouseUp = new MouseEvent('mouseup', {
            bubbles: true,
            cancelable: true,
            clientX: finalX,
            clientY: finalY,
            button: 0
        });

        const click = new MouseEvent('click', {
            bubbles: true,
            cancelable: true,
            clientX: finalX,
            clientY: finalY,
            button: 0
        });

        // Dispatch events with human-like timing
        target.dispatchEvent(mouseDown);
        setTimeout(() => {
            target.dispatchEvent(mouseUp);
            setTimeout(() => {
                target.dispatchEvent(click);
            }, Math.random() * 20 + 10); // 10-30ms delay
        }, Math.random() * 50 + 50); // 50-100ms delay
    }

    // --- Turnstile Token Capture (Stealth Integration) ---
    console.log('[Auth] Setting up Turnstile token capture...');

    // Store the original native function
    const originalCreateElement = document.createElement;

    // Overwrite the function with our temporary trap
    document.createElement = function(...args) {
        // Run the original function to create the element
        const element = originalCreateElement.apply(this, args);

        // Only interested in SCRIPT tags
        if (element.tagName === 'SCRIPT') {
            // Use a different approach - override setAttribute instead of src property
            const originalSetAttribute = element.setAttribute;
            element.setAttribute = function(name, value) {
                // Call the original setAttribute first
                originalSetAttribute.call(this, name, value);

                // If it's setting the src attribute and it's the turnstile script
                if (name === 'src' && value && value.includes('challenges.cloudflare.com/turnstile')) {
                    console.log('[Auth] Turnstile SCRIPT tag found! Adding load listener.');

                    // Add our 'load' event listener to hook the object AFTER execution
                    element.addEventListener('load', function() {
                        console.log('[Auth] Turnstile script has loaded. Now safe to hook turnstile.render().');
                        if (window.turnstile) {
                            hookTurnstileRender(window.turnstile);
                        }
                    });

                    // --- THIS IS THE CRITICAL STEP ---
                    // We have found our target, so we restore the original function immediately.
                    console.log('[Auth] Trap is no longer needed. Restoring original document.createElement.');
                    document.createElement = originalCreateElement;
                }
            };
        }
        return element;
    };

    function hookTurnstileRender(turnstile) {
        const originalRender = turnstile.render;
        turnstile.render = function(container, params) {
            console.log('[Auth] Intercepted turnstile.render() call.');
            const originalCallback = params.callback;
            params.callback = (token) => {
                handleTurnstileToken(token);
                if (originalCallback) return originalCallback(token);
            };
            return originalRender(container, params);
        };
    }

    function handleTurnstileToken(token) {
        latestTurnstileToken = token;
        const message = `✅ Cloudflare Turnstile Token Captured: ${token}`;
        console.log('%c' + message, 'color: #28a745; font-weight: bold;');

        console.log('[Auth] Fresh Turnstile token captured and ready for use.');
    }

    // Define the Turnstile onload callback function globally
    window.onloadTurnstileCallback = function() {
        console.log('[Auth] 🎯 Turnstile onload callback triggered');
        if (window.turnstile) {
            console.log('[Auth] 🔧 Turnstile object available, setting up hooks...');
            hookTurnstileRender(window.turnstile);

            // Create a hidden Turnstile widget to generate a token
            setTimeout(() => {
                createHiddenTurnstileWidget();
            }, 1000); // Wait a bit for hooks to be fully set up
        } else {
            console.warn('[Auth] ⚠️ Turnstile object not available in onload callback');
        }
    };

    function extractTurnstileSitekey() {
        // Use known LMArena sitekey directly
        const sitekey = '0x4AAAAAAA65vWDmG-O_lPtT';
        console.log('[Auth] 🔑 Using LMArena sitekey:', sitekey);
        return sitekey;
    }

    function createHiddenTurnstileWidget() {
        try {
            console.log('[Auth] 🎯 Creating hidden Turnstile widget to generate token...');

            // Extract the correct sitekey from the page
            const sitekey = extractTurnstileSitekey();
            if (!sitekey) {
                console.error('[Auth] ❌ Cannot create Turnstile widget: no sitekey found');
                return;
            }

            // Create a hidden container for the Turnstile widget
            const container = document.createElement('div');
            container.id = 'hidden-turnstile-widget';
            container.style.position = 'absolute';
            container.style.left = '-9999px';
            container.style.top = '-9999px';
            container.style.width = '300px';
            container.style.height = '65px';
            container.style.visibility = 'hidden';
            container.style.opacity = '0';
            container.style.pointerEvents = 'none';

            document.body.appendChild(container);

            // Render the Turnstile widget
            if (window.turnstile && window.turnstile.render) {
                const widgetId = window.turnstile.render(container, {
                    sitekey: sitekey,
                    callback: function(token) {
                        console.log('[Auth] 🎉 Hidden Turnstile widget generated token!');
                        handleTurnstileToken(token);
                    },
                    'error-callback': function(error) {
                        console.warn('[Auth] ⚠️ Hidden Turnstile widget error:', error);
                    },
                    'expired-callback': function() {
                        console.log('[Auth] ⏰ Hidden Turnstile token expired, creating new widget...');
                        // Remove old widget and create new one
                        const oldContainer = document.getElementById('hidden-turnstile-widget');
                        if (oldContainer) {
                            oldContainer.remove();
                        }
                        setTimeout(createHiddenTurnstileWidget, 1000);
                    },
                    theme: 'light',
                    size: 'normal'
                });

                console.log('[Auth] ✅ Hidden Turnstile widget created with ID:', widgetId);
            } else {
                console.error('[Auth] ❌ Turnstile render function not available');
            }
        } catch (error) {
            console.error('[Auth] ❌ Error creating hidden Turnstile widget:', error);
        }
    }

    // Consolidated authentication helper functions
    async function initializeTurnstileIfNeeded() {
        console.log("[Auth] 🔧 Initializing Turnstile API if needed...");

        try {
            const script = document.createElement('script');
            script.src = 'https://challenges.cloudflare.com/turnstile/v0/api.js?onload=onloadTurnstileCallback&render=explicit';
            script.async = true;
            script.defer = true;

            script.onload = () => {
                console.log("[Auth] ✅ Turnstile API script loaded successfully");
            };
            script.onerror = (error) => {
                console.warn("[Auth] ⚠️ Failed to load Turnstile API script:", error);
            };

            document.head.appendChild(script);
            console.log("[Auth] ✅ Turnstile API script injection initiated");

            // Schedule a human-like click 3 seconds after script injection to avoid bot detection
            setTimeout(() => {
                simulateHumanClick();
            }, 3000 + Math.random() * 1000); // 3-4 seconds with randomness
        } catch (error) {
            console.warn("[Auth] ⚠️ Failed to initialize Turnstile API:", error.message);
            throw error;
        }
    }

    async function ensureAuthenticationReady(requestId) {
        console.log(`[Auth] 🔐 Ensuring authentication is ready for request ${requestId}...`);

        // Check for required authentication cookie first
        if (!checkAuthCookie()) {
            console.log(`[Auth] ⚠️ Missing auth cookie for request ${requestId}, initiating auth flow...`);

            // Check if we have stored auth data first
            let authData = getStoredAuthData();

            if (!authData) {
                // No valid stored auth, need to authenticate
                // But first, double-check if auth cookie became available
                if (checkAuthCookie()) {
                    console.log(`[Auth] ✅ Auth cookie became available during auth check for request ${requestId} - skipping authentication`);
                    return; // Auth cookie is now available, no need to authenticate
                }

                let turnstileToken = latestTurnstileToken;

                if (!turnstileToken) {
                    console.log(`[Auth] ⏳ No Turnstile token available yet for request ${requestId}, initializing Turnstile API...`);

                    // Initialize Turnstile API if no token is available
                    await initializeTurnstileIfNeeded();

                    console.log(`[Auth] ⏳ Waiting for Turnstile token for request ${requestId}...`);
                    turnstileToken = await waitForTurnstileToken();

                    if (turnstileToken === 'auth_cookie_available') {
                        console.log(`[Auth] ✅ Auth cookie became available during wait for request ${requestId} - skipping authentication`);
                        return; // Auth cookie is now available, no need to authenticate
                    }

                    if (!turnstileToken) {
                        throw new Error("Authentication required: Turnstile token not generated within timeout. Please refresh the page.");
                    }
                }

                console.log(`[Auth] 🔑 Have Turnstile token for request ${requestId}, performing authentication...`);
                authData = await performAuthentication(turnstileToken);
            }

            console.log(`[Auth] ✅ Authentication complete for request ${requestId}`);
        } else {
            console.log(`[Auth] ✅ Auth cookie already present for request ${requestId}`);
        }
    }

    function getCookie(name) {
        const value = `; ${document.cookie}`;
        const parts = value.split(`; ${name}=`);
        if (parts.length === 2) return parts.pop().split(';').shift();
        return null;
    }

    function checkAuthCookie() {
        const authCookie = getCookie(REQUIRED_COOKIE);
        if (authCookie) {
            console.log(`[Auth] ✅ Found required cookie: ${REQUIRED_COOKIE}`);
            return true;
        } else {
            console.log(`[Auth] ❌ Missing required cookie: ${REQUIRED_COOKIE}`);
            return false;
        }
    }

    async function waitForTurnstileToken(maxWaitTime = 60000) {
        console.log(`[Auth] ⏳ Waiting for Turnstile token to be generated...`);

        const checkInterval = 1000; // Check every 1 second
        let waitTime = 0;

        while (waitTime < maxWaitTime) {
            // Check if auth cookie became available - if so, no need to wait for Turnstile token
            if (checkAuthCookie()) {
                console.log(`[Auth] ✅ Auth cookie became available during Turnstile wait after ${waitTime}ms - skipping token wait`);
                return 'auth_cookie_available';
            }

            if (latestTurnstileToken) {
                console.log(`[Auth] ✅ Turnstile token available after ${waitTime}ms`);
                return latestTurnstileToken;
            }

            console.log(`[Auth] ⏳ Still waiting for Turnstile token or auth cookie... (${waitTime}ms elapsed)`);
            await new Promise(resolve => setTimeout(resolve, checkInterval));
            waitTime += checkInterval;
        }

        console.warn(`[Auth] ⚠️ Turnstile token wait timeout after ${maxWaitTime}ms`);
        return null;
    }

    async function performAuthentication(turnstileToken) {
        console.log(`[Auth] 🔐 Starting authentication process with Turnstile token...`);

        try {
            // Step 1: Get JWT token from sign-up endpoint
            const authResponse = await fetch('https://lmarena.ai/api/sign-up', {
                method: 'POST',
                headers: {
                    'Content-Type': 'text/plain;charset=UTF-8',
                    'Accept': '*/*',
                    // The browser automatically adds critical headers:
                    // Cookie, User-Agent, Accept-Language, sec-ch-ua, etc.
                },
                body: JSON.stringify({
                    turnstile_token: turnstileToken
                })
            });

            if (!authResponse.ok) {
                throw new Error(`Authentication request failed with status ${authResponse.status}`);
            }

            const authData = await authResponse.json();
            console.log(`[Auth] ✅ Step 1: Received JWT token from sign-up.`);

            // Step 2: Create and set the auth cookie
            const cookieValue = `base64-${btoa(JSON.stringify(authData))}`;
            document.cookie = `${REQUIRED_COOKIE}=${cookieValue}; path=/; domain=.lmarena.ai; secure; samesite=lax`;
            console.log(`[Auth] ✅ Step 2: Set auth cookie in browser.`);

            // Step 3: Make verification request to complete authentication
            const verifyResponse = await fetch('https://lmarena.ai/', {
                method: 'POST',
                headers: {
                    'Content-Type': 'text/plain;charset=UTF-8',
                    'Accept': 'text/x-component',
                    'next-action': '40fc0d1f8f1f9cade86c05d74881beb408c1fbf3c4',
                    'next-router-state-tree': '%5B%22%22%2C%7B%22children%22%3A%5B%5B%22locale%22%2C%22en%22%2C%22d%22%5D%2C%7B%22children%22%3A%5B%22(app)%22%2C%7B%22children%22%3A%5B%22(with-sidebar)%22%2C%7B%22children%22%3A%5B%22__PAGE__%22%2C%7B%7D%2C%22%2F%22%2C%22refresh%22%5D%7D%5D%7D%2Cnull%2Cnull%2Ctrue%5D%7D%5D%7D%5D',
                    'x-deployment-id': 'dpl_55LQ8Z7ygwa1s99pJVeED1iU2jvW',
                    // The browser automatically adds critical headers including the new auth cookie
                },
                body: JSON.stringify([])
            });

            if (!verifyResponse.ok) {
                throw new Error(`Authentication verification failed with status ${verifyResponse.status}`);
            }

            const verifyText = await verifyResponse.text();
            console.log(`[Auth] ✅ Step 3: Authentication verification completed.`);
            console.log(`[Auth] Verification response:`, verifyText);

            // Store the authentication data
            localStorage.setItem('lmarena_auth_data', JSON.stringify(authData));
            localStorage.setItem('lmarena_auth_timestamp', Date.now().toString());

            console.log(`[Auth] 💾 Stored authentication data. Token expires at: ${new Date(authData.expires_at * 1000).toISOString()}`);
            console.log(`[Auth] 🎉 Complete authentication flow finished successfully!`);

            return authData;

        } catch (error) {
            console.error(`[Auth] ❌ Authentication failed:`, error);
            throw error;
        }
    }

    function getStoredAuthData() {
        const authData = localStorage.getItem('lmarena_auth_data');
        const timestamp = localStorage.getItem('lmarena_auth_timestamp');

        if (authData && timestamp) {
            try {
                const parsedAuthData = JSON.parse(authData);
                const currentTime = Math.floor(Date.now() / 1000); // Current time in seconds

                // Check if token is still valid (with 5 minute buffer)
                if (parsedAuthData.expires_at && (parsedAuthData.expires_at - 300) > currentTime) {
                    const remainingTime = parsedAuthData.expires_at - currentTime;
                    console.log(`[Auth] Using stored auth data (expires in ${Math.round(remainingTime/60)} minutes)`);
                    return parsedAuthData;
                } else {
                    console.log(`[Auth] Stored auth data expired, removing...`);
                    localStorage.removeItem('lmarena_auth_data');
                    localStorage.removeItem('lmarena_auth_timestamp');
                }
            } catch (error) {
                console.error(`[Auth] Error parsing stored auth data:`, error);
                localStorage.removeItem('lmarena_auth_data');
                localStorage.removeItem('lmarena_auth_timestamp');
            }
        }
        return null;
    }

    async function waitForCloudflareAuth() {
        console.log("[Injector] ⏳ Waiting for Cloudflare authentication to complete...");

        const maxWaitTime = 45000; // 45 seconds max wait (increased for slow CF challenges)
        const checkInterval = 500; // Check every 0.5 seconds (faster since we're not making network requests)
        let waitTime = 0;

        while (waitTime < maxWaitTime) {
            try {
                // Check the current page DOM for CF challenge indicators
                if (!isCurrentPageCloudflareChallenge()) {
                    console.log(`[Injector] ✅ Cloudflare authentication completed after ${waitTime}ms`);
                    return true;
                }

                console.log(`[Injector] ⏳ Still waiting for CF auth... (${waitTime}ms elapsed)`);

            } catch (error) {
                console.log(`[Injector] ⏳ CF auth check failed, continuing to wait... (${waitTime}ms elapsed) - ${error.message}`);
            }

            await new Promise(resolve => setTimeout(resolve, checkInterval));
            waitTime += checkInterval;
        }

        console.warn(`[Injector] ⚠️ CF authentication wait timeout after ${maxWaitTime}ms`);
        return false;
    }

    async function processPendingRequests() {
        // Retrieve pending requests from localStorage (survives page refresh)
        const storedRequests = localStorage.getItem('lmarena_pending_requests');
        if (!storedRequests) {
            console.log("[Injector] 📭 No pending requests found, skipping processing");
            return;
        }

        try {
            const requests = JSON.parse(storedRequests);
            if (!requests || requests.length === 0) {
                console.log("[Injector] 📭 No pending requests in storage, cleaning up");
                localStorage.removeItem('lmarena_pending_requests');
                return;
            }

            console.log(`[Injector] 🔄 Found ${requests.length} pending requests after refresh`);

                // CRITICAL: Wait for CF authentication to complete before processing requests
                console.log("[Injector] ⏳ Waiting for Cloudflare authentication to complete before processing requests...");
                const authComplete = await waitForCloudflareAuth();

                if (!authComplete) {
                    console.error("[Injector] ❌ CF authentication timeout - requests may fail");
                    console.log("[Injector] 🔍 CF auth failed, but NOT triggering refresh - only refreshing on actual 429/CF challenge");
                    // Don't process requests if CF auth failed
                    return;
                }

                console.log("[Injector] ✅ Cloudflare authentication completed, proceeding with requests");

                // Wait a bit more for the page to fully stabilize after CF auth
                await new Promise(resolve => setTimeout(resolve, 2000));

                // ENHANCED: Wait for either Turnstile token OR auth cookie to become available
                console.log("[Injector] ⏳ Waiting for either Turnstile token or auth cookie to become available...");
                const maxWaitTime = 60000; // 60 seconds max wait
                const checkInterval = 1000; // Check every 1 second
                let waitTime = 0;
                let authReady = false;
                let turnstileInitialized = false;

                while (waitTime < maxWaitTime && !authReady) {
                    // PRIORITY 1: Check if auth cookie already exists - if so, we're done!
                    if (checkAuthCookie()) {
                        console.log("[Injector] 🎉 Auth cookie found after refresh, ready to proceed!");
                        authReady = true;
                        break;
                    }

                    // PRIORITY 2: Only check for Turnstile token if we don't have auth cookie
                    if (latestTurnstileToken) {
                        console.log("[Injector] ✅ Turnstile token available, ready to proceed!");
                        authReady = true;
                        break;
                    }

                    // PRIORITY 3: Only initialize Turnstile if we don't have auth cookie and no token yet
                    if (!turnstileInitialized && waitTime > 2000) { // Wait 2 seconds before initializing
                        // Double-check auth cookie before initializing Turnstile
                        if (checkAuthCookie()) {
                            console.log("[Injector] 🎉 Auth cookie became available before Turnstile init, skipping!");
                            authReady = true;
                            break;
                        }

                        console.log("[Injector] 🔧 No auth cookie or Turnstile token found, initializing Turnstile API...");
                        try {
                            await initializeTurnstileIfNeeded();
                            turnstileInitialized = true;

                            // Check auth cookie again after Turnstile init in case it became available
                            if (checkAuthCookie()) {
                                console.log("[Injector] 🎉 Auth cookie became available during Turnstile init!");
                                authReady = true;
                                break;
                            }
                        } catch (error) {
                            console.warn("[Injector] ⚠️ Failed to initialize Turnstile API:", error.message);
                            turnstileInitialized = true; // Don't retry
                        }
                    }

                    console.log(`[Injector] ⏳ Still waiting for auth cookie or Turnstile token... (${waitTime}ms elapsed)`);
                    await new Promise(resolve => setTimeout(resolve, checkInterval));
                    waitTime += checkInterval;
                }

                if (!authReady) {
                    console.log("[Injector] ⚠️ Neither auth cookie nor Turnstile token became available within timeout, proceeding anyway");
                }

                // Wait for the reconnection handshake to complete
                await new Promise(resolve => setTimeout(resolve, 1000));

                // Clear the stored requests to prevent duplicate processing
                localStorage.removeItem('lmarena_pending_requests');

                // Process each pending request
                for (let i = 0; i < requests.length; i++) {
                    const request = requests[i];
                    const { requestId, payload, files_to_upload } = request;
                    console.log(`[Injector] 🔄 Retrying request ${requestId} after refresh (${i + 1}/${requests.length})`);

                    // Add a small delay between requests to avoid overwhelming the server
                    if (i > 0) {
                        await new Promise(resolve => setTimeout(resolve, 500));
                    }

                    try {
                        // Check if this is an upload request or regular request
                        if (files_to_upload && files_to_upload.length > 0) {
                            console.log(`[Injector] 🔄 Retrying upload request ${requestId} with ${files_to_upload.length} file(s)`);
                            await handleUploadAndChat(requestId, payload, files_to_upload);
                        } else {
                            console.log(`[Injector] 🔄 Retrying regular request ${requestId}`);
                            await executeFetchAndStreamBack(requestId, payload);
                        }
                        console.log(`[Injector] ✅ Successfully retried request ${requestId}`);
                    } catch (error) {
                        console.error(`[Injector] ❌ Failed to retry request ${requestId}:`, error);
                        // Send error to server so it can timeout the request
                        if (socket && socket.readyState === WebSocket.OPEN) {
                            socket.send(JSON.stringify({
                                request_id: requestId,
                                data: JSON.stringify({ error: `Retry failed: ${error.message}` })
                            }));
                            socket.send(JSON.stringify({
                                request_id: requestId,
                                data: "[DONE]"
                            }));
                        }
                    }
                }

                console.log(`[Injector] 🎉 Completed processing ${requests.length} pending requests`);

        } catch (error) {
            console.error("[Injector] ❌ Error processing pending requests:", error);
            localStorage.removeItem('lmarena_pending_requests');
        }
    }

    function connect() {
        console.log(`[Injector] Connecting to server at ${CONFIG.SERVER_URL}...`);
        socket = new WebSocket(CONFIG.SERVER_URL);

        socket.onopen = () => {
            console.log("[Injector] ✅ Connection established with local server.");

            // Send reconnection handshake if we have pending requests
            sendReconnectionHandshake();

            // Check if we have pending requests from before a CF refresh
            processPendingRequests();

            // Send model registry after connection
            if (!modelRegistrySent) {
                setTimeout(() => {
                    sendModelRegistry();
                }, 2000); // Wait a bit for page to fully load
            }
        };

        socket.onmessage = async (event) => {
            try {
                const message = JSON.parse(event.data);

                // 处理心跳 ping
                if (message.type === 'ping') {
                    console.log('[Injector] 💓 Received ping, sending pong...');
                    socket.send(JSON.stringify({
                        type: 'pong',
                        timestamp: message.timestamp
                    }));
                    return;
                }

                if (message.type === 'refresh_models') {
                    console.log('[Injector] 🔄 Received model refresh request');
                    sendModelRegistry();
                    return;
                }

                if (message.type === 'model_registry_ack') {
                    console.log(`[Injector] ✅ Model registry updated with ${message.count} models`);
                    modelRegistrySent = true;
                    return;
                }

                if (message.type === 'reconnection_ack') {
                    console.log(`[Injector] 🤝 Reconnection acknowledged: ${message.message}`);
                    if (message.pending_request_ids && message.pending_request_ids.length > 0) {
                        console.log(`[Injector] 📋 Server has ${message.pending_request_ids.length} pending requests waiting`);
                    }
                    return;
                }

                if (message.type === 'restoration_ack') {
                    console.log(`[Injector] 🔄 Request restoration acknowledged: ${message.message}`);
                    console.log(`[Injector] ✅ ${message.restored_count} request channels restored`);
                    return;
                }

                // Handle abort request from server when client disconnects
                if (message.type === 'abort_request') {
                    const requestId = message.request_id;
                    console.log(`[Injector] 🛑 Received abort request for ${requestId}`);

                    const controller = activeFetchControllers.get(requestId);
                    if (controller) {
                        controller.abort();
                        activeFetchControllers.delete(requestId);
                        console.log(`[Injector] ✅ Aborted fetch request ${requestId}`);
                    } else {
                        console.log(`[Injector] ⚠️ No active fetch found for request ${requestId}`);
                    }
                    return;
                }

                const { request_id, payload, files_to_upload } = message;

                if (!request_id || !payload) {
                    console.error("[Injector] Invalid message from server:", message);
                    return;
                }

                // Check if there are files to upload and route to the correct handler
                if (files_to_upload && files_to_upload.length > 0) {
                    console.log(`[Injector] ⬆️ Received request with ${files_to_upload.length} file(s). Starting upload process.`);
                    await handleUploadAndChat(request_id, payload, files_to_upload);
                } else {
                    console.log(`[Injector] ⬇️ Received standard text request ${request_id}. Firing fetch.`);
                    await executeFetchAndStreamBack(request_id, payload);
                }

            } catch (error) {
                console.error("[Injector] Error processing message from server:", error);
            }
        };

        socket.onclose = () => {
            console.warn("[Injector] 🔌 Connection to local server closed. Retrying in 5 seconds...");
            modelRegistrySent = false; // Reset flag on disconnect

            // Abort all active fetch requests when WebSocket closes
            if (activeFetchControllers.size > 0) {
                console.log(`[Injector] 🛑 Aborting ${activeFetchControllers.size} active fetch requests due to WebSocket disconnect`);
                for (const [requestId, controller] of activeFetchControllers) {
                    controller.abort();
                    console.log(`[Injector] ✅ Aborted fetch request ${requestId}`);
                }
                activeFetchControllers.clear();
            }

            setTimeout(connect, 5000);
        };

        socket.onerror = (error) => {
            console.error("[Injector] ❌ WebSocket error:", error);
            socket.close(); // This will trigger the onclose reconnect logic
        };
    }

    function isCloudflareChallenge(responseText) {
        // Check for common Cloudflare challenge indicators
        return responseText.includes('Checking your browser before accessing') ||
               responseText.includes('DDoS protection by Cloudflare') ||
               responseText.includes('cf-browser-verification') ||
               responseText.includes('cf-challenge-running') ||
               responseText.includes('__cf_chl_jschl_tk__') ||
               responseText.includes('cloudflare-static') ||
               responseText.includes('<title>Just a moment...</title>') ||
               responseText.includes('Enable JavaScript and cookies to continue') ||
               responseText.includes('window._cf_chl_opt') ||
               (responseText.includes('cloudflare') && responseText.includes('challenge'));
    }

    function isCurrentPageCloudflareChallenge() {
        // Check the current page DOM for CF challenge indicators
        try {
            // Check page title
            if (document.title.includes('Just a moment') ||
                document.title.includes('Checking your browser') ||
                document.title.includes('Please wait')) {
                console.log("[Injector] 🛡️ CF challenge detected in page title");
                return true;
            }

            // Check for CF challenge elements in the DOM
            const cfIndicators = [
                'cf-browser-verification',
                'cf-challenge-running',
                'cf-wrapper',
                'cf-error-details',
                'cloudflare-static'
            ];

            for (const indicator of cfIndicators) {
                if (document.getElementById(indicator) ||
                    document.querySelector(`[class*="${indicator}"]`) ||
                    document.querySelector(`[id*="${indicator}"]`)) {
                    console.log(`[Injector] 🛡️ CF challenge detected: found element with ${indicator}`);
                    return true;
                }
            }

            // Check for CF challenge text content
            const bodyText = document.body ? document.body.textContent || document.body.innerText : '';
            if (bodyText.includes('Checking your browser before accessing') ||
                bodyText.includes('DDoS protection by Cloudflare') ||
                bodyText.includes('Enable JavaScript and cookies to continue') ||
                bodyText.includes('Please complete the security check') ||
                bodyText.includes('Verifying you are human')) {
                console.log("[Injector] 🛡️ CF challenge detected in page text content");
                return true;
            }

            // Check for CF challenge scripts
            const scripts = document.querySelectorAll('script');
            for (const script of scripts) {
                const scriptContent = script.textContent || script.innerHTML;
                if (scriptContent.includes('__cf_chl_jschl_tk__') ||
                    scriptContent.includes('window._cf_chl_opt') ||
                    scriptContent.includes('cf_challenge_response')) {
                    console.log("[Injector] 🛡️ CF challenge detected in script content");
                    return true;
                }
            }

            // Check if the page looks like the normal LMArena interface
            // Look for key elements that should be present on the normal page
            const normalPageIndicators = [
                'nav', 'header', 'main',
                '[data-testid]', '[class*="chat"]', '[class*="model"]',
                'input[type="text"]', 'textarea'
            ];

            let normalElementsFound = 0;
            for (const selector of normalPageIndicators) {
                if (document.querySelector(selector)) {
                    normalElementsFound++;
                }
            }

            // If we found several normal page elements, it's likely not a CF challenge
            if (normalElementsFound >= 3) {
                console.log(`[Injector] ✅ Normal page detected: found ${normalElementsFound} normal elements`);
                return false;
            }

            // If the page is mostly empty or has very few elements, it might be a CF challenge
            const totalElements = document.querySelectorAll('*').length;
            if (totalElements < 50) {
                console.log(`[Injector] 🛡️ Possible CF challenge: page has only ${totalElements} elements`);
                return true;
            }

            console.log("[Injector] ✅ No CF challenge indicators found in current page");
            return false;

        } catch (error) {
            console.warn(`[Injector] ⚠️ Error checking current page for CF challenge: ${error.message}`);
            // If we can't check, assume no challenge to avoid blocking
            return false;
        }
    }

    async function handleCloudflareRefresh() {
        if (isRefreshing) {
            console.log("[Injector] 🔄 Already refreshing, skipping duplicate refresh request");
            return;
        }

        isRefreshing = true;
        console.log("[Injector] 🔄 Cloudflare challenge detected! Refreshing page to get new token...");

        try {
            // Check if we have any pending requests stored
            const storedRequests = localStorage.getItem('lmarena_pending_requests');
            if (storedRequests) {
                const requests = JSON.parse(storedRequests);
                console.log(`[Injector] 💾 Found ${requests.length} pending requests, refreshing page...`);
            }

            // Refresh the page to trigger new CF authentication
            window.location.reload();

            // Wait for page to reload and CF auth to complete
            // The script will restart after reload, so this won't continue
        } catch (error) {
            console.error("[Injector] ❌ Error during CF refresh:", error);
            isRefreshing = false;
        }
    }

    async function handleRateLimitRefresh() {
        if (isRefreshing) {
            console.log("[Injector] 🔄 Already refreshing, skipping duplicate rate limit refresh request");
            return;
        }

        isRefreshing = true;
        console.log("[Injector] 🚫 Rate limit (429) detected! Deleting auth cookie and refreshing to create new identity...");

        try {
            // Delete ALL cookies to create a completely fresh identity
            console.log(`[Injector] 🗑️ Deleting ALL cookies to ensure fresh identity...`);

            // Get all cookies and delete them
            const cookies = document.cookie.split(";");
            for (let cookie of cookies) {
                const eqPos = cookie.indexOf("=");
                const name = eqPos > -1 ? cookie.substr(0, eqPos).trim() : cookie.trim();
                if (name) {
                    // Delete for current domain
                    document.cookie = `${name}=; path=/; expires=Thu, 01 Jan 1970 00:00:00 GMT`;
                    // Delete for lmarena.ai domain
                    document.cookie = `${name}=; path=/; domain=lmarena.ai; expires=Thu, 01 Jan 1970 00:00:00 GMT`;
                    // Delete for .lmarena.ai domain
                    document.cookie = `${name}=; path=/; domain=.lmarena.ai; expires=Thu, 01 Jan 1970 00:00:00 GMT`;
                    // Delete for lmarena.ai domain
                    document.cookie = `${name}=; path=/; domain=lmarena.ai; expires=Thu, 01 Jan 1970 00:00:00 GMT`;
                }
            }
            console.log(`[Injector] 🗑️ Deleted ${cookies.length} cookies`);

            // Also clear stored auth data
            localStorage.removeItem('lmarena_auth_data');
            localStorage.removeItem('lmarena_auth_timestamp');
            console.log(`[Injector] 🗑️ Cleared stored auth data`);

            // Check if we have any pending requests stored
            const storedRequests = localStorage.getItem('lmarena_pending_requests');
            if (storedRequests) {
                const requests = JSON.parse(storedRequests);
                console.log(`[Injector] 💾 Found ${requests.length} pending requests, refreshing page...`);
            }

            // Refresh the page to get new identity
            window.location.reload();

            // Wait for page to reload and new auth to complete
            // The script will restart after reload, so this won't continue
        } catch (error) {
            console.error("[Injector] ❌ Error during rate limit refresh:", error);
            isRefreshing = false;
        }
    }

    // Helper function to convert base64 to a Blob
    function base64ToBlob(base64, contentType) {
        const byteCharacters = atob(base64);
        const byteNumbers = new Array(byteCharacters.length);
        for (let i = 0; i < byteCharacters.length; i++) {
            byteNumbers[i] = byteCharacters.charCodeAt(i);
        }
        const byteArray = new Uint8Array(byteNumbers);
        return new Blob([byteArray], { type: contentType });
    }

    async function handleUploadAndChat(requestId, payload, filesToUpload) {
        // Create abort controller for this request
        const abortController = new AbortController();
        activeFetchControllers.set(requestId, abortController);

        try {
            console.log(`[Uploader] 🚀 Starting upload and chat for request ${requestId}`);

            // Ensure authentication is ready before making requests
            await ensureAuthenticationReady(requestId);

            const attachments = [];
            for (const file of filesToUpload) {
                console.log(`[Uploader] Processing file: ${file.fileName}`);

                // Step 1: Get Signed URL
                console.log(`[Uploader] Step 1: Getting signed URL for ${file.fileName}`);
                const signUrlResponse = await fetch('https://lmarena.ai/?mode=direct', {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'text/plain;charset=UTF-8',
                        'Accept': 'text/x-component',
                        'next-action': '70921b78972dce2a8502e777b245519ec3b4304009',
                        'next-router-state-tree': '%5B%22%22%2C%7B%22children%22%3A%5B%5B%22locale%22%2C%22en%22%2C%22d%22%5D%2C%7B%22children%22%3A%5B%22(app)%22%2C%7B%22children%22%3A%5B%22(with-sidebar)%22%2C%7B%22children%22%3A%5B%22__PAGE__%3F%7B%5C%22mode%5C%22%3A%5C%22direct%5C%22%7D%22%2C%7B%7D%2C%22%2F%3Fmode%3Ddirect%22%2C%22refresh%22%5D%7D%2Cnull%2Cnull%5D%7D%2Cnull%2Cnull%2Ctrue%5D%7D%2Cnull%2Cnull%5D%7D%2Cnull%2Cnull%5D',
                        'origin': 'https://lmarena.ai',
                        'referer': 'https://lmarena.ai/'
                    },
                    body: JSON.stringify([file.fileName, file.contentType]),
                    signal: abortController.signal
                });

                const signUrlText = await signUrlResponse.text();
                console.log("[Uploader] Received for signed URL:", signUrlText);

                // The response format may vary. Try different parsing strategies.
                let signUrlData = null;

                // Strategy 1: Look for "1:{...}" pattern (original format)
                let match = signUrlText.match(/1:({.*})/);
                if (match && match.length >= 2) {
                    console.log("[Uploader] Found data with '1:' prefix");
                    signUrlData = JSON.parse(match[1]);
                } else {
                    // Strategy 2: Look for any numbered prefix pattern like "0:{...}", "2:{...}", etc.
                    match = signUrlText.match(/\d+:({.*})/);
                    if (match && match.length >= 2) {
                        console.log(`[Uploader] Found data with '${match[0].split(':')[0]}:' prefix`);
                        signUrlData = JSON.parse(match[1]);
                    } else {
                        // Strategy 3: Try to parse the entire response as JSON
                        try {
                            signUrlData = JSON.parse(signUrlText);
                            console.log("[Uploader] Parsed entire response as JSON");
                        } catch (e) {
                            // Strategy 4: Look for JSON objects in the response
                            const jsonMatches = signUrlText.match(/{[^}]*"uploadUrl"[^}]*}/g);
                            if (jsonMatches && jsonMatches.length > 0) {
                                signUrlData = JSON.parse(jsonMatches[0]);
                                console.log("[Uploader] Found JSON object containing uploadUrl");
                            } else {
                                throw new Error(`Could not parse signed URL response. Response: ${signUrlText}`);
                            }
                        }
                    }
                }

                if (!signUrlData || !signUrlData.data || !signUrlData.data.uploadUrl) {
                    throw new Error('Signed URL data is incomplete or invalid after parsing.');
                }
                const { uploadUrl, key } = signUrlData.data;
                console.log(`[Uploader] Got signed URL. Key: ${key}`);

                // Step 2: Upload file to storage
                console.log(`[Uploader] Step 2: Uploading file to cloud storage...`);
                const blob = base64ToBlob(file.data, file.contentType);
                const uploadResponse = await fetch(uploadUrl, {
                    method: 'PUT',
                    headers: { 'Content-Type': file.contentType },
                    body: blob,
                    signal: abortController.signal
                });
                if (!uploadResponse.ok) throw new Error(`File upload failed with status ${uploadResponse.status}`);
                console.log(`[Uploader] File uploaded successfully.`);

                // Step 3: Notify LMArena of upload
                console.log(`[Uploader] Step 3: Notifying LMArena of upload completion...`);
                const notifyResponse = await fetch('https://lmarena.ai/?mode=direct', {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'text/plain;charset=UTF-8',
                        'Accept': 'text/x-component',
                        'next-action': '6022a52184b31a48befd4697f8aa5cca57a0c68dd5',
                        'next-router-state-tree': '%5B%22%22%2C%7B%22children%22%3A%5B%5B%22locale%22%2C%22en%22%2C%22d%22%5D%2C%7B%22children%22%3A%5B%22(app)%22%2C%7B%22children%22%3A%5B%22(with-sidebar)%22%2C%7B%22children%22%3A%5B%22__PAGE__%3F%7B%5C%22mode%5C%22%3A%5C%22direct%5C%22%7D%22%2C%7B%7D%2C%22%2F%3Fmode%3Ddirect%22%2C%22refresh%22%5D%7D%2Cnull%2Cnull%5D%7D%2Cnull%2Cnull%2Ctrue%5D%7D%2Cnull%2Cnull%5D%7D%2Cnull%2Cnull%5D',
                        'origin': 'https://lmarena.ai',
                        'referer': 'https://lmarena.ai/'
                    },
                    body: JSON.stringify([key]),
                    signal: abortController.signal
                });

                const notifyText = await notifyResponse.text();
                console.log(`[Uploader] Notification sent. Response:`, notifyText);

                // Parse the response from the notification step to get the final URL
                const finalUrlDataLine = notifyText.split('\n').find(line => line.startsWith('1:'));
                if (!finalUrlDataLine) throw new Error('Could not find final URL data in notification response.');

                const finalUrlData = JSON.parse(finalUrlDataLine.substring(2));
                const finalUrl = finalUrlData.data.url;
                if (!finalUrl) throw new Error('Final URL not found in notification response data.');

                console.log(`[Uploader] Extracted final GetObject URL: ${finalUrl}`);

                attachments.push({
                    name: key,
                    contentType: file.contentType,
                    url: finalUrl
                });
            }

            // Step 4: Modify payload with attachments and send final request
            console.log('[Uploader] All files uploaded. Modifying final payload...');
            const userMessage = payload.messages.find(m => m.role === 'user');
            if (userMessage) {
                userMessage.experimental_attachments = attachments;
            } else {
                throw new Error("Could not find user message in payload to attach files to.");
            }

            console.log('[Uploader] Payload modified. Initiating final chat stream.');
            await executeFetchAndStreamBack(requestId, payload);

        } catch (error) {
            if (error.name === 'AbortError') {
                console.log(`[Uploader] Upload process aborted for request ${requestId}`);
                // Don't send error back to server since client has disconnected
            } else {
                console.error(`[Uploader] Error during file upload process for request ${requestId}:`, error);
                // === 添加429错误处理 ===
                // 检查是否是429错误（可能在上传文件的任何步骤中发生）
                if (error.message && error.message.includes('429')) {
                    console.log(`[Uploader] 🚫 Rate limit detected during upload`);

                    const existingRequests = JSON.parse(localStorage.getItem('lmarena_pending_requests') || '[]');
                    const alreadyStored = existingRequests.some(req => req.requestId === requestId);

                    if (!alreadyStored) {
                        existingRequests.push({
                            requestId,
                            payload,
                            files_to_upload: filesToUpload  // 保存文件信息！这是关键
                        });
                        localStorage.setItem('lmarena_pending_requests', JSON.stringify(existingRequests));
                        console.log(`[Uploader] 💾 Stored upload request ${requestId} with ${filesToUpload.length} files for retry`);
                    }

                    handleRateLimitRefresh();
                    return;
                }
                sendToServer(requestId, JSON.stringify({ error: `File upload failed: ${error.message}` }));
                sendToServer(requestId, "[DONE]");
            }
        } finally {
            // Clean up abort controller
            activeFetchControllers.delete(requestId);
        }
    }

    async function executeFetchAndStreamBack(requestId, payload) {
        // Create abort controller for this request
        const abortController = new AbortController();
        activeFetchControllers.set(requestId, abortController);

        try {
            console.log(`[Injector] 🚀 Starting fetch for request ${requestId}`);

            // Ensure authentication is ready before making request
            await ensureAuthenticationReady(requestId);

            const response = await fetch(`https://lmarena.ai${TARGET_API_PATH}`, {
                method: 'POST',
                headers: {
                    'Content-Type': 'text/plain;charset=UTF-8',
                    'Accept': '*/*',
                    // The browser automatically adds critical headers:
                    // Cookie, User-Agent, sec-ch-ua, etc.
                },
                body: JSON.stringify(payload),
                signal: abortController.signal
            });

            // Check for rate limit (429) error first
            if (response.status === 429) {
                console.log(`[Injector] 🚫 Rate limit (429) detected for request ${requestId}`);

                // Check if this request is already stored to prevent duplicates
                const existingRequests = JSON.parse(localStorage.getItem('lmarena_pending_requests') || '[]');
                const alreadyStored = existingRequests.some(req => req.requestId === requestId);

                if (!alreadyStored) {
                    existingRequests.push({
                        requestId,
                        payload,
                        files_to_upload: []  // 普通请求没有文件
                    });
                    localStorage.setItem('lmarena_pending_requests', JSON.stringify(existingRequests));
                    console.log(`[Injector] 💾 Stored request ${requestId} for retry after rate limit refresh`);
                } else {
                    console.log(`[Injector] ⚠️ Request ${requestId} already stored, skipping duplicate`);
                }

                // Trigger rate limit refresh (don't await to prevent blocking)
                handleRateLimitRefresh();
                return; // Function will not continue after page refresh
            }

            // Check if we got a Cloudflare challenge instead of the expected response
            if (!response.ok || response.headers.get('content-type')?.includes('text/html')) {
                const responseText = await response.text();

                if (isCloudflareChallenge(responseText)) {
                    console.log(`[Injector] 🛡️ Cloudflare challenge detected for request ${requestId} (Status: ${response.status})`);

                    // Check if this request is already stored to prevent duplicates
                    const existingRequests = JSON.parse(localStorage.getItem('lmarena_pending_requests') || '[]');
                    const alreadyStored = existingRequests.some(req => req.requestId === requestId);

                    if (!alreadyStored) {
                        existingRequests.push({ requestId, payload });
                        localStorage.setItem('lmarena_pending_requests', JSON.stringify(existingRequests));
                        console.log(`[Injector] 💾 Stored request ${requestId} for retry after CF refresh`);
                    } else {
                        console.log(`[Injector] ⚠️ Request ${requestId} already stored, skipping duplicate`);
                    }

                    // Trigger automatic refresh (don't await to prevent blocking)
                    handleCloudflareRefresh();
                    return; // Function will not continue after page refresh
                }

                // If it's not a CF challenge, treat as regular error
                throw new Error(`Fetch failed with status ${response.status}: ${responseText}`);
            }

            if (!response.body) {
                throw new Error(`No response body received for request ${requestId}`);
            }

            console.log(`[Injector] 📡 Starting to stream response for request ${requestId}`);
            const reader = response.body.getReader();
            const decoder = new TextDecoder();

            while (true) {
                // Check if we've been aborted before reading
                if (abortController.signal.aborted) {
                    console.log(`[Injector] Stream aborted for request ${requestId}, cancelling reader`);
                    await reader.cancel();
                    break;
                }

                const { value, done } = await reader.read();
                if (done) {
                    console.log(`[Injector] ✅ Stream finished for request ${requestId}.`);
                    sendToServer(requestId, "[DONE]");
                    break;
                }

                const chunk = decoder.decode(value);

                // Additional check: if we get HTML in the stream, it might be a CF challenge
                if (chunk.includes('<html') || chunk.includes('<!DOCTYPE')) {
                    if (isCloudflareChallenge(chunk)) {
                        console.log(`[Injector] 🛡️ Cloudflare challenge detected in stream for request ${requestId}`);

                        // Check if this request is already stored to prevent duplicates
                        const existingRequests = JSON.parse(localStorage.getItem('lmarena_pending_requests') || '[]');
                        const alreadyStored = existingRequests.some(req => req.requestId === requestId);

                        if (!alreadyStored) {
                            existingRequests.push({ requestId, payload });
                            localStorage.setItem('lmarena_pending_requests', JSON.stringify(existingRequests));
                            console.log(`[Injector] 💾 Stored request ${requestId} for retry after CF refresh (detected in stream)`);
                        }

                        // Trigger automatic refresh (don't await to prevent blocking)
                        handleCloudflareRefresh();
                        return;
                    }
                }

                // Check abort signal again before sending data
                if (abortController.signal.aborted) {
                    console.log(`[Injector] Stream aborted for request ${requestId}, stopping data transmission`);
                    await reader.cancel();
                    break;
                }

                // The stream often sends multiple lines in one chunk
                const lines = chunk.split('\n').filter(line => line.trim() !== '');
                for (const line of lines) {
                    // One more check before each send
                    if (abortController.signal.aborted) {
                        console.log(`[Injector] Aborting mid-chunk for request ${requestId}`);
                        await reader.cancel();
                        return; // Exit the entire function
                    }
                    sendToServer(requestId, line);
                }
            }

        } catch (error) {
            if (error.name === 'AbortError') {
                console.log(`[Injector] Fetch aborted for request ${requestId}`);
                // Don't send error back to server since client has disconnected
            } else {
                console.error(`[Injector] ❌ Error during fetch for request ${requestId}:`, error);
                sendToServer(requestId, JSON.stringify({ error: error.message }));
                sendToServer(requestId, "[DONE]"); // Ensure the stream is always terminated
            }
        } finally {
            // Clean up abort controller
            activeFetchControllers.delete(requestId);
        }
    }

    function sendToServer(requestId, data) {
        // Check if this request has been aborted
        const controller = activeFetchControllers.get(requestId);
        if (controller && controller.signal.aborted) {
            console.log(`[Injector] Not sending data for aborted request ${requestId}`);
            return;
        }

        if (socket && socket.readyState === WebSocket.OPEN) {
            const message = {
                request_id: requestId,
                data: data
            };
            socket.send(JSON.stringify(message));
        } else {
            console.error("[Injector] Cannot send data, socket is not open.");
        }
    }

    function extractModelRegistry() {
        console.log('[Injector] 🔍 Extracting model registry from script tags...');

        try {
            const scripts = document.querySelectorAll('script');
            let modelData = null;
            const searchString = CONFIG.MODEL_REGISTRY.SEARCH_STRING;

            for (const script of scripts) {
                const content = script.textContent || script.innerHTML;
                if (content.includes(searchString)) {
                    console.log('[Injector] Found the target script tag.');

                    // The entire payload is inside this one script's content.
                    const startIndex = content.indexOf(searchString);
                    const payloadStartIndex = startIndex + searchString.length; // Start at the first '[' after the ':'

                    let balance = 1;
                    let endIndex = -1;
                    let inString = false;

                    // Start searching from after the first bracket
                    for (let i = payloadStartIndex + 1; i < content.length; i++) {
                        const char = content[i];
                        const prevChar = content[i-1];

                        // Toggle inString state if we encounter a quote that is not escaped
                        if (char === '"' && prevChar !== '\\') {
                            inString = !inString;
                        }

                        // Only count brackets if we're not inside a string
                        if (!inString) {
                            if (char === '[') {
                                balance++;
                            } else if (char === ']') {
                                balance--;
                            }
                        }

                        // If balance is zero, we've found the end of our JSON object
                        if (balance === 0) {
                            endIndex = i + 1;
                            break;
                        }
                    }

                    if (endIndex === -1) {
                        console.error('[Injector] Could not find the end of the JSON payload using bracket counting.');
                        continue;
                    }

                    const rawPayload = content.substring(payloadStartIndex, endIndex);
                    const jsonString = rawPayload.replace(/\\"/g, '"').replace(/\\\\/g, '\\');

                    console.log('[Injector] Payload extracted via bracket counting. Parsing JSON...');
                    const parsedData = JSON.parse(jsonString);

                    function findAllInitialStates(obj) {
                        let allModels = [];
                        function search(currentObj) {
                            if (!currentObj || typeof currentObj !== 'object') return;
                            if (currentObj.hasOwnProperty('initialState') && Array.isArray(currentObj.initialState)) {
                                if (currentObj.initialState.length > 0 && currentObj.initialState[0] && currentObj.initialState[0].publicName) {
                                    allModels = allModels.concat(currentObj.initialState);
                                }
                            }
                            for (const key in currentObj) {
                                if (currentObj.hasOwnProperty(key)) search(currentObj[key]);
                            }
                        }
                        search(obj);
                        return allModels;
                    }

                    modelData = findAllInitialStates(parsedData);
                    if (modelData && modelData.length > 0) {
                        console.log(`[Injector] Successfully aggregated ${modelData.length} models.`);
                        break; // Exit after processing the correct script
                    }
                }
            }

            if (!modelData || modelData.length === 0) {
                console.warn('[Injector] Model extraction failed.');
                return null;
            }

            const registry = {};
            modelData.forEach(model => {
                if (!model || typeof model !== 'object' || !model.publicName) return;
                if (registry[model.publicName]) return;

                let type = 'chat';
                if (model.capabilities && model.capabilities.outputCapabilities) {
                    if (model.capabilities.outputCapabilities.image) type = 'image';
                    else if (model.capabilities.outputCapabilities.video) type = 'video';
                }

                registry[model.publicName] = { type: type, ...model };
            });

            console.log(`[Injector] ✅ Extracted ${Object.keys(registry).length} unique models.`);
            return registry;

        } catch (error) {
            console.error('[Injector] ❌ Error extracting model registry:', error);
            return null;
        }
    }

    function sendReconnectionHandshake() {
        if (!socket || socket.readyState !== WebSocket.OPEN) {
            console.log('[Injector] ⚠️ WebSocket not ready, cannot send reconnection handshake');
            return;
        }

        // Get pending request IDs from localStorage
        const storedRequests = localStorage.getItem('lmarena_pending_requests');
        let pendingRequestIds = [];

        if (storedRequests) {
            try {
                const requests = JSON.parse(storedRequests);
                pendingRequestIds = requests.map(req => req.requestId);
                console.log(`[Injector] 🤝 Sending reconnection handshake with ${pendingRequestIds.length} pending requests`);
            } catch (error) {
                console.error("[Injector] Error parsing stored requests for handshake:", error);
            }
        }

        const handshakeMessage = {
            type: 'reconnection_handshake',
            pending_request_ids: pendingRequestIds,
            timestamp: Date.now()
        };

        socket.send(JSON.stringify(handshakeMessage));
        console.log(`[Injector] 📤 Sent reconnection handshake`);
    }

    function sendModelRegistry() {
        if (!socket || socket.readyState !== WebSocket.OPEN) {
            console.log('[Injector] ⚠️ WebSocket not ready, cannot send model registry');
            return;
        }

        const models = extractModelRegistry();

        if (models && Object.keys(models).length > 0) {
            const message = {
                type: 'model_registry',
                models: models
            };

            socket.send(JSON.stringify(message));
            console.log(`[Injector] 📤 Sent model registry with ${Object.keys(models).length} models`);
        } else {
            console.warn('[Injector] ⚠️ No models extracted, not sending registry');
        }
    }

    // --- Start the connection ---
    connect();

    // Also try to send model registry when page loads
    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', () => {
            setTimeout(sendModelRegistry, 3000);
        });
    } else {
        setTimeout(sendModelRegistry, 3000);
    }

})();