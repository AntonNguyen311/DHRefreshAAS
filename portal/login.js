(function () {
    const config = window.portalConfig;
    const dashboardPath = "./dashboard.html";

    const elements = {
        signInButton: document.getElementById("signInButton"),
        openDashboardButton: document.getElementById("openDashboardButton"),
        loginStatus: document.getElementById("loginStatus")
    };

    const state = {
        msalApp: null,
        account: null
    };

    function ensureConfig() {
        if (!config || !config.auth || !config.api) {
            throw new Error("portalConfig is missing. Update portal/config.js before using the portal.");
        }
    }

    async function initialize() {
        ensureConfig();

        state.msalApp = new msal.PublicClientApplication({
            auth: config.auth,
            cache: {
                cacheLocation: "sessionStorage"
            }
        });

        await state.msalApp.initialize();
        await state.msalApp.handleRedirectPromise();

        const accounts = state.msalApp.getAllAccounts();
        if (accounts.length > 0) {
            state.account = accounts[0];
            state.msalApp.setActiveAccount(state.account);
        }

        bindEvents();
        updateUi();

        if (state.account) {
            elements.loginStatus.textContent = "Session found. Opening dashboard...";
            window.location.replace(dashboardPath);
        }
    }

    function bindEvents() {
        elements.signInButton.addEventListener("click", signIn);
        elements.openDashboardButton.addEventListener("click", openDashboard);
    }

    async function signIn() {
        try {
            elements.loginStatus.textContent = "Signing in...";
            const response = await state.msalApp.loginPopup({
                scopes: ["openid", "profile", "email", config.api.scope]
            });

            state.account = response.account || state.msalApp.getAllAccounts()[0] || null;
            state.msalApp.setActiveAccount(state.account);
            updateUi();
            window.location.replace(dashboardPath);
        } catch (error) {
            setError(error);
        }
    }

    function openDashboard() {
        if (!state.account) {
            elements.loginStatus.textContent = "Sign in first to open the dashboard.";
            return;
        }

        window.location.replace(dashboardPath);
    }

    function updateUi() {
        const signedIn = !!state.account;
        elements.openDashboardButton.disabled = !signedIn;
        if (signedIn) {
            const label = state.account.username || state.account.name || "unknown user";
            elements.loginStatus.textContent = "Signed in as " + label;
        }
    }

    function setError(error) {
        const message = error instanceof Error ? error.message : String(error);
        elements.loginStatus.textContent = message;
    }

    initialize().catch(setError);
})();
