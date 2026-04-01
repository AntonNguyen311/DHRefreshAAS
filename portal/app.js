(function () {
    const config = window.portalConfig;
    const loginPath = "./index.html";
    const state = {
        msalApp: null,
        account: null,
        latestOperationId: "",
        models: [],
        tables: [],
        partitions: [],
        recentOperations: []
    };

    const elements = {
        backToLoginButton: document.getElementById("backToLoginButton"),
        signOutButton: document.getElementById("signOutButton"),
        sessionStatus: document.getElementById("sessionStatus"),
        dashboardUserEmail: document.getElementById("dashboardUserEmail"),
        messageBanner: document.getElementById("messageBanner"),
        modelsCount: document.getElementById("modelsCount"),
        partitionsCount: document.getElementById("partitionsCount"),
        recentOperationsCount: document.getElementById("recentOperationsCount"),
        userProfile: document.getElementById("userProfile"),
        modelSelect: document.getElementById("modelSelect"),
        tableSelect: document.getElementById("tableSelect"),
        partitionSelect: document.getElementById("partitionSelect"),
        refreshTypeSelect: document.getElementById("refreshTypeSelect"),
        wholeTableCheckbox: document.getElementById("wholeTableCheckbox"),
        loadMetadataButton: document.getElementById("loadMetadataButton"),
        submitRefreshButton: document.getElementById("submitRefreshButton"),
        selectionHint: document.getElementById("selectionHint"),
        submitStatus: document.getElementById("submitStatus"),
        submitPayload: document.getElementById("submitPayload"),
        operationStatus: document.getElementById("operationStatus"),
        pollLatestButton: document.getElementById("pollLatestButton"),
        loadHistoryButton: document.getElementById("loadHistoryButton"),
        historyContainer: document.getElementById("historyContainer")
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
        updateMetricCounts();

        if (!state.account) {
            redirectToLogin();
            return;
        }

        updateSessionUi();
        await loadModels();
        await loadHistory();
    }

    function bindEvents() {
        elements.backToLoginButton.addEventListener("click", goToLogin);
        elements.signOutButton.addEventListener("click", signOut);
        elements.loadMetadataButton.addEventListener("click", loadModels);
        elements.modelSelect.addEventListener("change", onModelChanged);
        elements.tableSelect.addEventListener("change", onTableChanged);
        elements.wholeTableCheckbox.addEventListener("change", updatePartitionUiState);
        elements.submitRefreshButton.addEventListener("click", submitRefresh);
        elements.pollLatestButton.addEventListener("click", pollLatestOperation);
        elements.loadHistoryButton.addEventListener("click", loadHistory);
    }

    function redirectToLogin() {
        window.location.replace(loginPath);
    }

    function goToLogin() {
        window.location.href = loginPath;
    }

    async function signOut() {
        const currentAccount = state.account;
        state.account = null;
        state.latestOperationId = "";
        state.models = [];
        state.tables = [];
        state.partitions = [];
        resetSelections();
        updateSessionUi();

        if (currentAccount) {
            await state.msalApp.logoutPopup({ account: currentAccount });
        }

        redirectToLogin();
    }

    async function acquireApiToken() {
        if (!state.account) {
            throw new Error("No signed-in account.");
        }

        try {
            const tokenResponse = await state.msalApp.acquireTokenSilent({
                account: state.account,
                scopes: [config.api.scope]
            });

            return tokenResponse.accessToken;
        } catch (error) {
            const message = error && error.message ? error.message.toLowerCase() : "";
            const requiresInteraction =
                (error && error.name === "InteractionRequiredAuthError") ||
                message.includes("interaction_required") ||
                message.includes("consent_required") ||
                message.includes("login_required");

            if (!requiresInteraction) {
                throw error;
            }

            const tokenResponse = await state.msalApp.acquireTokenPopup({
                account: state.account,
                scopes: [config.api.scope]
            });

            return tokenResponse.accessToken;
        }
    }

    async function apiFetch(path, options) {
        const token = await acquireApiToken();
        let response;
        try {
            response = await fetch(config.api.baseUrl + path, {
                ...options,
                headers: {
                    "Authorization": "Bearer " + token,
                    "Content-Type": "application/json",
                    ...(options && options.headers ? options.headers : {})
                }
            });
        } catch (error) {
            throw new Error("The dashboard could not reach the portal API. Please refresh and try again.");
        }

        const text = await response.text();
        let data = {};
        if (text) {
            try {
                data = JSON.parse(text);
            } catch {
                data = { raw: text };
            }
        }

        if (!response.ok) {
            const detailMessage = data && data.details ? String(data.details) : "";
            const baseMessage =
                (data && (data.error || data.message || data.raw)) ||
                response.statusText ||
                "Request failed.";
            const message = detailMessage && !baseMessage.includes(detailMessage)
                ? baseMessage + ": " + detailMessage
                : baseMessage;
            throw new Error(message);
        }

        return data;
    }

    async function loadModels() {
        try {
            setBusy("Loading models...");
            const data = await apiFetch("/api/DHRefreshAAS_PortalModels", { method: "GET" });
            state.models = data.models || [];
            fillSelect(elements.modelSelect, state.models.map(item => ({
                value: item.databaseName,
                label: item.databaseName + " (" + item.allowedTableCount + " tables)"
            })), "Select a model");
            elements.modelSelect.disabled = state.models.length === 0;
            elements.loadMetadataButton.disabled = false;
            elements.loadHistoryButton.disabled = false;
            elements.selectionHint.textContent = state.models.length > 0
                ? "Select a model to load allowed tables."
                : "No models are currently available for your account.";
            elements.userProfile.textContent = JSON.stringify(data.user || {}, null, 2);
            updateMetricCounts();
            setBanner(
                state.models.length > 0 ? "success" : "warning",
                state.models.length > 0
                    ? "Metadata loaded successfully."
                    : "Signed in successfully, but no models are currently available for your account."
            );
            updateActionAvailability();
        } catch (error) {
            setError(error);
        }
    }

    async function onModelChanged() {
        resetTableAndPartition();
        const databaseName = elements.modelSelect.value;
        if (!databaseName) {
            return;
        }

        try {
            setBusy("Loading tables...");
            const data = await apiFetch("/api/DHRefreshAAS_PortalTables?databaseName=" + encodeURIComponent(databaseName), { method: "GET" });
            state.tables = data.tables || [];
            fillSelect(elements.tableSelect, state.tables.map(item => ({
                value: item.tableName,
                label: item.tableName + " (" + item.partitionCount + " partitions)"
            })), "Select a table");
            elements.tableSelect.disabled = state.tables.length === 0;
            elements.refreshTypeSelect.disabled = state.tables.length === 0;
            elements.selectionHint.textContent = state.tables.length > 0
                ? "Select a table to load partitions."
                : "No allowed tables were returned for the selected model.";
            setBanner(
                state.tables.length > 0 ? "success" : "warning",
                state.tables.length > 0
                    ? "Tables loaded successfully."
                    : "The selected model does not currently expose any allowed tables."
            );
            updateActionAvailability();
        } catch (error) {
            setError(error);
        }
    }

    async function onTableChanged() {
        state.partitions = [];
        fillSelect(elements.partitionSelect, [], "Select a partition");
        const databaseName = elements.modelSelect.value;
        const tableName = elements.tableSelect.value;
        if (!databaseName || !tableName) {
            return;
        }

        try {
            setBusy("Loading partitions...");
            const data = await apiFetch(
                "/api/DHRefreshAAS_PortalPartitions?databaseName=" + encodeURIComponent(databaseName) + "&tableName=" + encodeURIComponent(tableName),
                { method: "GET" });

            const payload = data.data || {};
            state.partitions = payload.partitions || [];
            const selectedTable = state.tables.find(item => item.tableName === tableName);
            elements.refreshTypeSelect.value = payload.defaultRefreshType || (selectedTable ? selectedTable.defaultRefreshType : "Full");
            elements.wholeTableCheckbox.checked = !!payload.supportsTableRefresh;
            elements.wholeTableCheckbox.disabled = !payload.supportsTableRefresh;
            fillSelect(elements.partitionSelect, state.partitions.map(item => ({
                value: item.partitionName,
                label: item.partitionName
            })), "Select a partition");
            updatePartitionUiState();
            updateMetricCounts();
            setBanner("success", "Partitions loaded successfully.");
            updateActionAvailability();
        } catch (error) {
            setError(error);
        }
    }

    function updatePartitionUiState() {
        const wholeTable = elements.wholeTableCheckbox.checked && !elements.wholeTableCheckbox.disabled;
        elements.partitionSelect.disabled = wholeTable || state.partitions.length === 0;
    }

    async function submitRefresh() {
        const databaseName = elements.modelSelect.value;
        const tableName = elements.tableSelect.value;
        const wholeTable = elements.wholeTableCheckbox.checked && !elements.wholeTableCheckbox.disabled;
        const partitionName = wholeTable ? "" : elements.partitionSelect.value;

        if (!databaseName || !tableName) {
            setError(new Error("Choose a model and table before submitting."));
            return;
        }

        if (!wholeTable && !partitionName) {
            setError(new Error("Choose a partition or enable whole table refresh."));
            return;
        }

        const payload = {
            databaseName: databaseName,
            refreshObjects: [
                {
                    table: tableName,
                    partition: partitionName,
                    refreshType: elements.refreshTypeSelect.value
                }
            ]
        };

        try {
            setBusy("Submitting refresh...");
            elements.submitPayload.textContent = JSON.stringify(payload, null, 2);
            const response = await apiFetch("/api/DHRefreshAAS_PortalSubmitRefresh", {
                method: "POST",
                body: JSON.stringify(payload)
            });
            state.latestOperationId = response.operationId || "";
            elements.submitStatus.textContent = response.message || "Refresh accepted.";
            elements.operationStatus.textContent = JSON.stringify(response, null, 2);
            elements.pollLatestButton.disabled = !state.latestOperationId;
            setBanner("success", elements.submitStatus.textContent);
            await loadHistory();
        } catch (error) {
            setError(error);
        }
    }

    async function pollLatestOperation() {
        if (!state.latestOperationId) {
            return;
        }

        try {
            setBusy("Polling latest operation...");
            const data = await apiFetch("/api/DHRefreshAAS_PortalStatus?operationId=" + encodeURIComponent(state.latestOperationId), { method: "GET" });
            elements.operationStatus.textContent = JSON.stringify(data, null, 2);
            setBanner("info", "Latest operation status refreshed.");
        } catch (error) {
            setError(error);
        }
    }

    async function loadHistory() {
        if (!state.account) {
            return;
        }

        try {
            const data = await apiFetch("/api/DHRefreshAAS_PortalStatus", { method: "GET" });
            state.recentOperations = data.recentOperations || [];
            renderHistory(state.recentOperations);
            updateMetricCounts();
        } catch (error) {
            setError(error);
        }
    }

    function renderHistory(operations) {
        if (!operations.length) {
            elements.historyContainer.innerHTML = "<p class=\"muted\">No recent operations found.</p>";
            return;
        }

        elements.historyContainer.innerHTML = operations.map(item => {
            const requestedBy = item.requestedBy && item.requestedBy.email ? item.requestedBy.email : "n/a";
            return [
                "<article class=\"history-item\">",
                "<h3>" + escapeHtml(item.operationId) + "</h3>",
                "<p><strong>Status:</strong> " + escapeHtml(item.status) + "</p>",
                "<p><strong>Tables:</strong> " + escapeHtml(String(item.tablesCount)) + "</p>",
                "<p><strong>Elapsed:</strong> " + escapeHtml(String(item.elapsedMinutes)) + " minutes</p>",
                "<p><strong>Requester:</strong> " + escapeHtml(requestedBy) + "</p>",
                "<button type=\"button\" class=\"secondary\" onclick=\"window.loadPortalOperation('" + escapeHtml(item.operationId) + "')\">View</button>",
                "</article>"
            ].join("");
        }).join("");
    }

    window.loadPortalOperation = async function (operationId) {
        state.latestOperationId = operationId;
        elements.pollLatestButton.disabled = false;
        await pollLatestOperation();
    };

    function fillSelect(selectElement, items, placeholder) {
        const options = ["<option value=\"\">" + escapeHtml(placeholder) + "</option>"]
            .concat(items.map(item => "<option value=\"" + escapeHtml(item.value) + "\">" + escapeHtml(item.label) + "</option>"));
        selectElement.innerHTML = options.join("");
    }

    function resetSelections() {
        fillSelect(elements.modelSelect, [], "Select a model");
        elements.modelSelect.disabled = true;
        resetTableAndPartition();
        elements.refreshTypeSelect.value = "Full";
        elements.refreshTypeSelect.disabled = true;
        elements.loadMetadataButton.disabled = true;
        elements.loadHistoryButton.disabled = true;
        elements.pollLatestButton.disabled = true;
        elements.submitRefreshButton.disabled = true;
        elements.historyContainer.innerHTML = "<p class=\"muted\">No history loaded.</p>";
        elements.operationStatus.textContent = "No operation selected.";
        elements.selectionHint.textContent = "Sign in to load available models and partitions.";
        updateMetricCounts();
    }

    function resetTableAndPartition() {
        state.tables = [];
        state.partitions = [];
        fillSelect(elements.tableSelect, [], "Select a table");
        fillSelect(elements.partitionSelect, [], "Select a partition");
        elements.tableSelect.disabled = true;
        elements.partitionSelect.disabled = true;
        elements.wholeTableCheckbox.checked = true;
        elements.wholeTableCheckbox.disabled = false;
        updateMetricCounts();
    }

    function updateActionAvailability() {
        const hasModel = !!elements.modelSelect.value;
        const hasTable = !!elements.tableSelect.value;
        const canUseWholeTable = !elements.wholeTableCheckbox.disabled && elements.wholeTableCheckbox.checked;
        const hasPartition = !!elements.partitionSelect.value;
        elements.submitRefreshButton.disabled = !(hasModel && hasTable && (canUseWholeTable || hasPartition));
    }

    function updateSessionUi() {
        const signedIn = !!state.account;
        elements.signOutButton.disabled = !signedIn;
        elements.sessionStatus.textContent = signedIn
            ? "Signed in as " + (state.account.username || state.account.name || "unknown user")
            : "Signed out";
        elements.dashboardUserEmail.textContent = signedIn
            ? (state.account.username || state.account.name || "unknown user")
            : "Unknown";

        if (!signedIn) {
            elements.userProfile.textContent = "No user profile loaded.";
            resetSelections();
        }
    }

    function updateMetricCounts() {
        elements.modelsCount.textContent = String(state.models.length);
        elements.partitionsCount.textContent = String(state.partitions.length);
        elements.recentOperationsCount.textContent = String(state.recentOperations.length);
    }

    function setBanner(kind, message) {
        elements.messageBanner.hidden = false;
        elements.messageBanner.className = "banner " + kind;
        elements.messageBanner.textContent = message;
    }

    function setBusy(message) {
        elements.submitStatus.textContent = message;
        setBanner("info", message);
    }

    function setError(error) {
        const message = error instanceof Error ? error.message : String(error);
        elements.submitStatus.textContent = message;
        elements.operationStatus.textContent = JSON.stringify({ error: message }, null, 2);
        setBanner("error", message);
    }

    function escapeHtml(value) {
        return String(value)
            .replaceAll("&", "&amp;")
            .replaceAll("<", "&lt;")
            .replaceAll(">", "&gt;")
            .replaceAll("\"", "&quot;")
            .replaceAll("'", "&#39;");
    }

    initialize().catch(setError);
})();
