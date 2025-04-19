// Main application state
const appState = {
    rankings: {},
    rankingChanges: {},
    currentOpportunities: { long: [], short: [] },
    historicalOpportunities: { long: [], short: [] },
    backtestResults: null,
    backtestSummary: null,
    slotMachineData: null,
    slotMachineMatchingAlgorithm: "consistent",
    allPairs: [],
    currentTimeframe: "1h",
    opportunityDisplayMode: "current",
    tableViewMode: "normal",
    sortBy: "overall_rank",
    sortAscending: true,
    isRunning: true,
    isWebSocketActive: true,
    lastPrices: {}
};

// Socket.io connection
let socket;

// Document ready
$(document).ready(function() {
    console.log("Initializing Trading Analysis System...");
    
    // Initialize Socket.io
    initSocketIO();
    
    // Initialize modals
    initModals();
    
    // Initialize keyboard shortcuts
    initKeyboardShortcuts();
    
    // Initialize view toggle buttons
    initViewToggleButtons();
    
    // Hide loading indicator after initialization
    hideLoadingIndicator();
});

// Initialize Socket.io connection
function initSocketIO() {
    socket = io();
    
    socket.on('connect', function() {
        console.log('Connected to server');
        showStatusMessage('Connected to server', 'success');
        
        // Request initial data
        socket.emit('request_data', {
            timeframe: appState.currentTimeframe
        });
    });
    
    socket.on('disconnect', function() {
        console.log('Disconnected from server');
        showStatusMessage('Disconnected from server', 'error');
        
        $('#websocket-status').text('Inactive').removeClass('green').addClass('red');
    });
    
    socket.on('status', function(data) {
        console.log('Status update:', data);
        
        // Update app state
        appState.isRunning = data.status === 'running';
        appState.isWebSocketActive = data.websocket_active;
        appState.currentTimeframe = data.current_timeframe;
        
        // Update UI
        $('#current-timeframe').text(appState.currentTimeframe);
        
        if (appState.isWebSocketActive) {
            $('#websocket-status').text('Active').removeClass('red').addClass('green');
        } else {
            $('#websocket-status').text('Inactive').removeClass('green').addClass('red');
        }
    });
    
    socket.on('initial_data', function(data) {
        console.log('Initial data received:', data);
        
        if (data.status === 'success') {
            // Update app state
            appState.currentTimeframe = data.timeframe;
            $('#current-timeframe').text(appState.currentTimeframe);
            
            // Update rankings
            if (data.rankings) {
                appState.rankings = {};
                data.rankings.forEach(ranking => {
                    appState.rankings[ranking.symbol] = ranking;
                });
            }
            
            // Update opportunities
            if (data.opportunities) {
                appState.currentOpportunities = data.opportunities;
                appState.historicalOpportunities = data.opportunities;
            }
            
            // Update UI
            updateUI();
        } else if (data.status === 'loading') {
            showStatusMessage('Loading data...', 'info');
        } else if (data.status === 'error') {
            showStatusMessage('Error loading data: ' + data.message, 'error');
        }
    });
    
    socket.on('kline_update', function(data) {
        // Update the last price
        if (data.symbol && data.close) {
            appState.lastPrices[data.symbol] = data.close;
        }
    });
    
    socket.on('trade_update', function(data) {
        // Update the last price
        if (data.symbol && data.price) {
            appState.lastPrices[data.symbol] = data.price;
        }
    });
    
    socket.on('analysis_update', function(data) {
        console.log('Analysis update received:', data);
        
        // Update app state
        if (data.timeframe !== appState.currentTimeframe) {
            return; // Ignore updates for different timeframes
        }
        
        if (data.rankings) {
            appState.rankings = data.rankings;
        }
        
        if (data.opportunities) {
            appState.currentOpportunities = data.opportunities;
            appState.historicalOpportunities = data.opportunities;
        }
        
        // Update UI
        updateUI();
    });
    
    socket.on('slot_machine_update', function(data) {
        console.log('Slot machine update received:', data);
        
        // Update app state
        if (data.timeframe !== appState.currentTimeframe) {
            return; // Ignore updates for different timeframes
        }
        
        if (data.slot_machine) {
            appState.slotMachineData = data.slot_machine;
        }
        
        // Update UI if in slot machine mode
        if (appState.tableViewMode === 'slotMachine') {
            updateSlotMachineTable();
        }
    });
}

// Initialize modals
function initModals() {
    // Setup modal close buttons
    $('.close-button').on('click', function() {
        $(this).closest('.modal').hide();
    });
    
    // Setup modals to close when clicking outside
    $(window).on('click', function(event) {
        if ($(event.target).hasClass('modal')) {
            $(event.target).hide();
        }
    });
    
    // Setup timeframe modal
    $('.option-button[data-timeframe]').on('click', function() {
        const timeframe = $(this).data('timeframe');
        appState.currentTimeframe = timeframe;
        
        // Update UI
        $('#current-timeframe').text(timeframe);
        
        // Close modal
        $('#timeframe-modal').hide();
        
        // Notify server
        socket.emit('change_timeframe', { timeframe: timeframe });
        
        // Show loading indicator
        showLoadingIndicator('Changing timeframe...');
        
        // Request updated data
        socket.emit('request_data', { timeframe: timeframe });
    });
    
    // Setup sort modal
    $('.option-button[data-sort]').on('click', function() {
        const sortBy = $(this).data('sort');
        appState.sortBy = sortBy;
        
        // Update UI
        $('#current-sort').text(sortBy);
        
        // Update selected option
        $('.option-button[data-sort]').removeClass('selected');
        $(this).addClass('selected');
        
        // Close modal
        $('#sort-modal').hide();
        
        // Update UI
        updateUI();
    });
    
    // Setup sort direction buttons
    $('#sort-asc').on('click', function() {
        appState.sortAscending = true;
        
        // Update UI
        $('#sort-direction').text('↑');
        
        // Update selected button
        $('#sort-asc').addClass('selected');
        $('#sort-desc').removeClass('selected');
        
        // Update UI
        updateUI();
    });
    
    $('#sort-desc').on('click', function() {
        appState.sortAscending = false;
        
        // Update UI
        $('#sort-direction').text('↓');
        
        // Update selected button
        $('#sort-asc').removeClass('selected');
        $('#sort-desc').addClass('selected');
        
        // Update UI
        updateUI();
    });
    
    // Setup backtest modal
    $('#run-backtest-btn').on('click', function() {
        // Get form values
        const symbols = $('#backtest-symbols').val();
        const timeframe = $('#backtest-timeframe').val();
        const takeProfit = parseFloat($('#backtest-take-profit').val());
        const stopLoss = parseFloat($('#backtest-stop-loss').val());
        const maxBars = parseInt($('#backtest-max-bars').val());
        
        // Validate
        if (takeProfit <= 0 || stopLoss <= 0 || maxBars <= 0) {
            showStatusMessage('Invalid parameters', 'error');
            return;
        }
        
        // Close modal
        $('#backtest-modal').hide();
        
        // Show loading indicator
        showLoadingIndicator('Running backtest...');
        
        // Run backtest
        $.ajax({
            url: '/api/backtest',
            type: 'POST',
            contentType: 'application/json',
            data: JSON.stringify({
                symbols: symbols ? symbols.split(',') : [],
                timeframe: timeframe,
                take_profit: takeProfit,
                stop_loss: stopLoss,
                max_bars: maxBars
            }),
            success: function(response) {
                hideLoadingIndicator();
                
                if (response.status === 'error') {
                    showStatusMessage('Error running backtest: ' + response.error, 'error');
                    return;
                }
                
                showStatusMessage('Backtest completed!', 'success');
                
                // Load backtest results
                loadBacktestResults(response.backtest_id);
            },
            error: function(xhr, status, error) {
                hideLoadingIndicator();
                showStatusMessage('Error running backtest: ' + error, 'error');
            }
        });
    });
}

// Initialize keyboard shortcuts
function initKeyboardShortcuts() {
    $(document).on('keydown', function(event) {
        if (!appState.isRunning) return;
        
        switch(event.key) {
            case 'q':
                // In a web app, q doesn't make as much sense, but we'll implement it for completeness
                if (confirm('Are you sure you want to quit the application?')) {
                    appState.isRunning = false;
                    showStatusMessage('Application stopped. Reload the page to restart.', 'info');
                }
                break;
                
            case 'r':
                // Refresh data
                refreshData();
                break;
                
            case 't':
                // Change timeframe
                $('#timeframe-modal').show();
                break;
                
            case 's':
                // Change sort
                $('#sort-modal').show();
                break;
                
            case 'm':
                // Toggle table view mode
                appState.tableViewMode = appState.tableViewMode === "normal" ? "slotMachine" : "normal";
                updateUI();
                showStatusMessage(`Switched to ${appState.tableViewMode} view`, "info");
                break;
                
            case 'w':
                // Toggle WebSocket
                toggleWebSocket();
                break;
                
            case 'o':
                // Toggle opportunity display mode
                appState.opportunityDisplayMode = appState.opportunityDisplayMode === "current" ? "historical" : "current";
                updateOpportunities();
                showStatusMessage(`Switched to ${appState.opportunityDisplayMode} opportunities view`, "info");
                break;
                
            case 'b':
                // Run backtest
                $('#backtest-modal').show();
                break;
        }
    });
}

// Initialize view toggle buttons
function initViewToggleButtons() {
    $('#normal-view-btn').on('click', function() {
        appState.tableViewMode = 'normal';
        
        // Update UI
        updateUI();
        
        // Update button states
        $('#normal-view-btn').addClass('active');
        $('#slot-machine-view-btn').removeClass('active');
        
        // Hide algorithm selection
        $('#algorithm-container').addClass('hidden');
        $('#consistent-algo-btn').addClass('hidden');
        $('#momentum-algo-btn').addClass('hidden');
        
        showStatusMessage('Switched to Normal view', 'info');
    });
    
    $('#slot-machine-view-btn').on('click', function() {
        appState.tableViewMode = 'slotMachine';
        
        // Update UI
        updateUI();
        
        // Update button states
        $('#normal-view-btn').removeClass('active');
        $('#slot-machine-view-btn').addClass('active');
        
        // Show algorithm selection
        $('#algorithm-container').removeClass('hidden');
        $('#consistent-algo-btn').removeClass('hidden');
        $('#momentum-algo-btn').removeClass('hidden');
        
        showStatusMessage('Switched to Slot Machine view', 'info');
    });
    
    $('#consistent-algo-btn').on('click', function() {
        appState.slotMachineMatchingAlgorithm = 'consistent';
        
        // Update button states
        $('#consistent-algo-btn').addClass('active');
        $('#momentum-algo-btn').removeClass('active');
        
        // Reload slot machine data
        loadSlotMachineData();
    });
    
    $('#momentum-algo-btn').on('click', function() {
        appState.slotMachineMatchingAlgorithm = 'momentum';
        
        // Update button states
        $('#consistent-algo-btn').removeClass('active');
        $('#momentum-algo-btn').addClass('active');
        
        // Reload slot machine data
        loadSlotMachineData();
    });
}

// Refresh data
function refreshData() {
    console.log('Refresh command received');
    
    // Show loading indicator
    showLoadingIndicator('Refreshing data...');
    
    // Request updated data
    socket.emit('request_data', { timeframe: appState.currentTimeframe });
    
    // Analyze data
    $.ajax({
        url: '/api/analyze',
        type: 'POST',
        contentType: 'application/json',
        data: JSON.stringify({
            timeframe: appState.currentTimeframe
        }),
        success: function(response) {
            hideLoadingIndicator();
            
            if (response.status === 'error') {
                showStatusMessage('Error analyzing data: ' + response.error, 'error');
                return;
            }
            
            showStatusMessage('Data refreshed successfully!', 'success');
        },
        error: function(xhr, status, error) {
            hideLoadingIndicator();
            showStatusMessage('Error analyzing data: ' + error, 'error');
        }
    });
}

// Toggle WebSocket
function toggleWebSocket() {
    appState.isWebSocketActive = !appState.isWebSocketActive;
    
    // Update server
    socket.emit('toggle_websocket', { active: appState.isWebSocketActive });
    
    // Update UI
    if (appState.isWebSocketActive) {
        $('#websocket-status').text('Active').removeClass('red').addClass('green');
        showStatusMessage('WebSocket activated', 'success');
    } else {
        $('#websocket-status').text('Inactive').removeClass('green').addClass('red');
        showStatusMessage('WebSocket deactivated', 'info');
    }
}

// Load backtest results
function loadBacktestResults(backtestId) {
    // Show loading indicator
    showLoadingIndicator('Loading backtest results...');
    
    $.ajax({
        url: `/api/backtest/${backtestId}`,
        type: 'GET',
        success: function(response) {
            hideLoadingIndicator();
            
            // Store in app state
            appState.backtestResults = response.results;
            
            // Format summary
            if (response.summary) {
                appState.backtestSummary = {
                    long: response.summary.find(s => s.direction === 'long'),
                    short: response.summary.find(s => s.direction === 'short'),
                    combined: response.summary.find(s => s.direction === 'combined')
                };
            }
            
            // Show backtest panel
            $('#backtest-panel').removeClass('hidden');
            
            // Update UI
            updateBacktestUI();
            
            showStatusMessage('Backtest results loaded!', 'success');
        },
        error: function(xhr, status, error) {
            hideLoadingIndicator();
            showStatusMessage('Error loading backtest results: ' + error, 'error');
        }
    });
}

// Load slot machine data
function loadSlotMachineData() {
    if (appState.tableViewMode !== 'slotMachine') {
        return;
    }
    
    // Show loading indicator
    showLoadingIndicator('Loading slot machine data...');
    
    $.ajax({
        url: `/api/slot-machine/${appState.currentTimeframe}`,
        type: 'GET',
        data: {
            algorithm: appState.slotMachineMatchingAlgorithm,
            max_rows: 50
        },
        success: function(response) {
            hideLoadingIndicator();
            
            // Store in app state
            appState.slotMachineData = response;
            
            // Update UI
            updateSlotMachineTable();
            
            showStatusMessage('Slot machine data loaded!', 'success');
        },
        error: function(xhr, status, error) {
            hideLoadingIndicator();
            showStatusMessage('Error loading slot machine data: ' + error, 'error');
        }
    });
}

// Update all UI components
function updateUI() {
    // Update current view text
    $('#current-view').text(appState.tableViewMode);
    
    // Update current sort text
    $('#current-sort').text(appState.sortBy);
    $('#sort-direction').text(appState.sortAscending ? '↑' : '↓');
    
    // Update the main table based on view mode
    if (appState.tableViewMode === 'slotMachine') {
        // Load or update slot machine data if needed
        if (!appState.slotMachineData) {
            loadSlotMachineData();
        } else {
            updateSlotMachineTable();
        }
        
        // Update button states
        $('#normal-view-btn').removeClass('active');
        $('#slot-machine-view-btn').addClass('active');
        
        // Show algorithm selection
        $('#algorithm-container').removeClass('hidden');
        $('#consistent-algo-btn').removeClass('hidden').addClass(appState.slotMachineMatchingAlgorithm === 'consistent' ? 'active' : '');
        $('#momentum-algo-btn').removeClass('hidden').addClass(appState.slotMachineMatchingAlgorithm === 'momentum' ? 'active' : '');
    } else {
        updateMainTable();
        
        // Update button states
        $('#normal-view-btn').addClass('active');
        $('#slot-machine-view-btn').removeClass('active');
        
        // Hide algorithm selection
        $('#algorithm-container').addClass('hidden');
        $('#consistent-algo-btn').addClass('hidden');
        $('#momentum-algo-btn').addClass('hidden');
    }
    
    updateOpportunities();
    
    // Update backtest UI if we have results
    if (appState.backtestSummary) {
        $('#backtest-panel').removeClass('hidden');
        updateBacktestUI();
    }
}

// Update the normal main table
function updateMainTable() {
    const tableContainer = $('#main-table');
    
    // Check if we have rankings
    if (!appState.rankings || Object.keys(appState.rankings).length === 0) {
        tableContainer.html('<div class="loading-message">No data available</div>');
        return;
    }
    
    // Start building the table HTML
    let tableHtml = `
        <table>
            <thead>
                <tr>
                    <th class="cyan" style="text-align: right; width: 60px;">Rank</th>
                    <th class="green">Symbol</th>
                    <th style="text-align: right;">Price</th>
                    <th style="text-align: right;">Volume ▼▲</th>
                    <th style="text-align: right;">Momentum ▼▲</th>
                    <th style="text-align: right;">Price % ▼▲</th>
                    <th style="text-align: right;">Total % ▼▲</th>
                    <th style="text-align: right;">Z-Score ▼▲</th>
                    <th style="text-align: center; width: 70px;">Trend</th>
                </tr>
            </thead>
            <tbody>
    `;
    
    // Sort the rankings
    const sortedItems = Object.values(appState.rankings)
        .sort((a, b) => {
            const valA = a[appState.sortBy] || 0;
            const valB = b[appState.sortBy] || 0;
            return appState.sortAscending ? valA - valB : valB - valA;
        });
    
    // Add table rows
    const maxDisplayed = sortedItems.length;
    
    sortedItems.slice(0, maxDisplayed).forEach(item => {
        // Get ranking changes if available
        const changes = appState.rankingChanges && appState.rankingChanges[item.symbol] 
            ? appState.rankingChanges[item.symbol] : {};
        
        // Format rank with change
        let rankText = `${item.overall_rank}`;
        let rankStyle = "";
        if (changes.overall_rank_change) {
            rankText += ` (${changes.overall_rank_change > 0 ? '+' + changes.overall_rank_change : changes.overall_rank_change})`;
            rankStyle = changes.overall_rank_change > 0 ? 'green' : changes.overall_rank_change < 0 ? 'red' : '';
        }
        
        // Format volumes with change
        let volumeText = `${parseFloat(item.volume_metric).toFixed(2)} / ${item.volume_rank}`;
        let volumeStyle = "";
        if (changes.volume_rank_change) {
            volumeText += ` (${changes.volume_rank_change > 0 ? '+' + changes.volume_rank_change : changes.volume_rank_change})`;
            volumeStyle = changes.volume_rank_change > 0 ? 'green' : changes.volume_rank_change < 0 ? 'red' : '';
        }
        
        // Format momentum with change
        let momentumText = `${parseFloat(item.momentum_metric).toFixed(2)} / ${item.momentum_rank}`;
        let momentumStyle = "";
        if (changes.momentum_rank_change) {
            momentumText += ` (${changes.momentum_rank_change > 0 ? '+' + changes.momentum_rank_change : changes.momentum_rank_change})`;
            momentumStyle = changes.momentum_rank_change > 0 ? 'green' : changes.momentum_rank_change < 0 ? 'red' : '';
        }
        
        // Format price metric with change
        let priceMetricText = `${parseFloat(item.price_metric).toFixed(2)}% / ${item.price_rank}`;
        let priceMetricStyle = "";
        if (changes.price_rank_change) {
            priceMetricText += ` (${changes.price_rank_change > 0 ? '+' + changes.price_rank_change : changes.price_rank_change})`;
            priceMetricStyle = changes.price_rank_change > 0 ? 'green' : changes.price_rank_change < 0 ? 'red' : '';
        }
        
        // Format total percent change with change
        let totalPctText = `${parseFloat(item.total_pct_change).toFixed(2)}% / ${item.total_pct_rank}`;
        let totalPctStyle = "";
        if (changes.total_pct_rank_change) {
            totalPctText += ` (${changes.total_pct_rank_change > 0 ? '+' + changes.total_pct_rank_change : changes.total_pct_rank_change})`;
            totalPctStyle = changes.total_pct_rank_change > 0 ? 'green' : changes.total_pct_rank_change < 0 ? 'red' : '';
        }
        
        // Format zscore with change
        let zscoreText = `${parseFloat(item.zscore_metric).toFixed(2)} / ${item.zscore_rank}`;
        let zscoreStyle = "";
        if (changes.zscore_rank_change) {
            zscoreText += ` (${changes.zscore_rank_change > 0 ? '+' + changes.zscore_rank_change : changes.zscore_rank_change})`;
            zscoreStyle = changes.zscore_rank_change > 0 ? 'green' : changes.zscore_rank_change < 0 ? 'red' : '';
        }
        
        // Format trend
        let trendText = item.in_uptrend ? "▲ UP" : "▼ DOWN";
        let trendStyle = item.in_uptrend ? 'trend-up' : 'trend-down';
        
        // Add the row
        tableHtml += `
            <tr>
                <td class="${rankStyle}" style="text-align: right;">${rankText}</td>
                <td>${item.symbol}</td>
                <td style="text-align: right;">${parseFloat(item.price).toFixed(4)}</td>
                <td class="${volumeStyle}" style="text-align: right;">${volumeText}</td>
                <td class="${momentumStyle}" style="text-align: right;">${momentumText}</td>
                <td class="${priceMetricStyle}" style="text-align: right;">${priceMetricText}</td>
                <td class="${totalPctStyle}" style="text-align: right;">${totalPctText}</td>
                <td class="${zscoreStyle}" style="text-align: right;">${zscoreText}</td>
                <td class="${trendStyle}" style="text-align: center;">${trendText}</td>
            </tr>
        `;
    });
    
    // Close the table
    tableHtml += `
            </tbody>
        </table>
    `;
    
    // Update the container
    tableContainer.html(tableHtml);
}

// Update the slot machine table
function updateSlotMachineTable() {
    const tableContainer = $('#main-table');
    
    // Skip if no slot machine data
    if (!appState.slotMachineData || !appState.slotMachineData.columns || appState.slotMachineData.columns.length === 0) {
        tableContainer.html('<div class="loading-message">No slot machine data available</div>');
        return;
    }
    
    // Check if we have any matches
    const hasMatches = appState.slotMachineData.matches && appState.slotMachineData.matches.length > 0;
    
    // Get matches by symbol for easy lookup
    const matchesBySymbol = {};
    const matchesBySymbolAndRank = {};
    
    if (hasMatches) {
        appState.slotMachineData.matches.forEach(match => {
            if (!matchesBySymbol[match.symbol]) {
                matchesBySymbol[match.symbol] = [];
            }
            matchesBySymbol[match.symbol].push(match);
            
            // Also store by symbol and rank for quicker lookup
            const key = `${match.symbol}_${match.rank}`;
            matchesBySymbolAndRank[key] = match;
        });
    }
    
    // Start building the table HTML - REMOVED SYMBOL COLUMN
    let tableHtml = `
        <table>
            <thead>
                <tr>
                    <th class="cyan" style="text-align: right; width: 40px;">Rank</th>
                    <th style="text-align: center;">Overall</th>
                    <th style="text-align: center;">Volume</th>
                    <th style="text-align: center;">Momentum</th>
                    <th style="text-align: center;">Price%</th>
                    <th style="text-align: center;">Total%</th>
                    <th style="text-align: center;">Z-Score</th>
                    <th style="text-align: center; width: 70px;">Trend</th>
                </tr>
            </thead>
            <tbody>
    `;
    
    // Determine max rows
    const maxRows = Math.min(50, appState.slotMachineData.columns[0]?.items.length || 0);
    
    // Create an array to track metrics
    const slotMachineMetrics = [
        {id: 'overall_rank', name: 'Overall'},
        {id: 'volume_rank', name: 'Volume'},
        {id: 'momentum_rank', name: 'Momentum'},
        {id: 'price_rank', name: 'Price%'},
        {id: 'total_pct_rank', name: 'Total%'},
        {id: 'zscore_rank', name: 'Z-Score'},
        {id: 'in_uptrend', name: 'Trend', format: 'trend'}
    ];
    
    // Process data for each row
    for (let row = 0; row < maxRows; row++) {
        tableHtml += '<tr>';
        
        // Add rank column
        tableHtml += `<td style="text-align: right;">${row + 1}</td>`;
        
        // Add each column's data for this row
        slotMachineMetrics.forEach(metric => {
            // Find the column data
            const column = appState.slotMachineData.columns.find(col => col.id === metric.id);
            
            // Skip if no column data or not enough items
            if (!column || !column.items || row >= column.items.length) {
                tableHtml += `<td>-</td>`;
                return;
            }
            
            // Get the item for this row
            const item = column.items[row];
            if (!item) {
                tableHtml += `<td>-</td>`;
                return;
            }
            
            // Determine cell content and styling
            let cellContent;
            let cellClass = '';
            
            if (metric.format === 'trend') {
                // Trend column shows UP/DOWN with color
                cellContent = item.in_uptrend ? "▲ UP" : "▼ DOWN";
                cellClass = item.in_uptrend ? 'trend-up' : 'trend-down';
            } else {
                // For other columns, show the symbol
                cellContent = item.symbol;
                
                // Check if this coin and rank matches with other columns
                const rankValue = row + 1; // 1-based rank for display
                const matchKey = `${item.symbol}_${rankValue}`;
                
                if (matchesBySymbolAndRank[matchKey]) {
                    const match = matchesBySymbolAndRank[matchKey];
                    if (match.columns && match.columns.includes(metric.id)) {
                        // Apply appropriate styling based on match count
                        if (match.matchCount >= 5) {
                            cellClass += ' jackpot';
                        } else if (match.matchCount >= 4) {
                            cellClass += ' slot-machine-match';
                        } else {
                            cellClass += ' highlight-cell';
                        }
                    }
                }
            }
            
            // Add the cell
            tableHtml += `<td class="${cellClass}" style="text-align: center;">${cellContent}</td>`;
        });
        
        tableHtml += '</tr>';
    }
    
    // Close the table
    tableHtml += `
            </tbody>
        </table>
    `;
    
    // Show matches info if any matches found
    if (hasMatches && appState.slotMachineData.matches.length > 0) {
        tableHtml += `
            <div style="margin-top: 20px; padding: 10px; background-color: rgba(78, 154, 6, 0.1); border: 1px solid #4e9a06;">
                <div class="bold green">Matching Coins (${appState.slotMachineData.matches.length}) - 
                    ${appState.slotMachineMatchingAlgorithm === 'consistent' ? 
                        'Consistent Top Rankings' : 
                        'Momentum Breakthrough'}:
                </div>
                <table style="margin-top: 10px;">
                    <thead>
                        <tr>
                            <th>Symbol</th>
                            <th>${appState.slotMachineMatchingAlgorithm === 'consistent' ? 'Overall Rank' : 'Overall Improvement'}</th>
                            <th>${appState.slotMachineMatchingAlgorithm === 'consistent' ? 'Top 20 Metrics' : 'Improved Metrics'}</th>
                            <th>Match Count</th>
                        </tr>
                    </thead>
                    <tbody>
        `;
        
        // Show up to 10 matches
        appState.slotMachineData.matches.slice(0, 10).forEach(match => {
            // Add match-specific display content based on algorithm
            if (appState.slotMachineMatchingAlgorithm === 'consistent') {
                // Get readable metric names
                const metricNames = match.columns.map(colId => {
                    const metricName = colId.replace('_rank', '');
                    return metricName.charAt(0).toUpperCase() + metricName.slice(1);
                }).join(', ');
                
                // Add match row for Consistent Top Rankings
                tableHtml += `
                    <tr>
                        <td class="bold ${match.matchCount >= 5 ? 'green' : 'white'}">${match.symbol}</td>
                        <td>${match.rank}</td>
                        <td>${metricNames}</td>
                        <td>${match.matchCount}</td>
                    </tr>
                `;
            } else {
                // Get readable metric names for improved metrics
                const metricNames = match.columns.map(colId => {
                    const metricName = colId.replace('_rank', '');
                    return metricName.charAt(0).toUpperCase() + metricName.slice(1);
                }).join(', ');
                
                // Add match row for Momentum Breakthrough
                tableHtml += `
                    <tr>
                        <td class="bold ${match.matchCount >= 4 ? 'green' : 'white'}">${match.symbol}</td>
                        <td class="green">+${match.overallImprovement}</td>
                        <td>${metricNames}</td>
                        <td>${match.matchCount}</td>
                    </tr>
                `;
            }
        });
        
        tableHtml += `
                    </tbody>
                </table>
            </div>
        `;
    }
    
    // Update the container
    tableContainer.html(tableHtml);
}

// Update the opportunities panel
function updateOpportunities() {
    const opportunitiesPanel = $('#opportunities-panel');
    
    // Determine which opportunities to display
    const oppsToDisplay = appState.opportunityDisplayMode === "current"
        ? appState.currentOpportunities
        : appState.historicalOpportunities;
    
    const panelTitle = appState.opportunityDisplayMode === "current"
        ? "Current Trading Opportunities"
        : "Historical Trading Opportunities";
    
    // Start building the panel HTML
    let panelHtml = `
        <div class="panel-title">${panelTitle} (Longs: ${oppsToDisplay.long.length} | Shorts: ${oppsToDisplay.short.length})</div>
    `;
    
    // Check if we have any opportunities
    if (!oppsToDisplay || (!oppsToDisplay.long.length && !oppsToDisplay.short.length)) {
        panelHtml += `<div class="loading-message">No opportunities detected</div>`;
        opportunitiesPanel.html(panelHtml);
        return;
    }
    
    // Continue building HTML with table
    panelHtml += `
        <table class="opportunities-table">
            <thead>
                <tr>
                    <th>Type</th>
                    <th>Symbol</th>
                    <th style="text-align: right;">Strength</th>
                    <th style="text-align: right;">Price %</th>
                    ${appState.opportunityDisplayMode === "historical" ? '<th>Time</th>' : ''}
                </tr>
            </thead>
            <tbody>
    `;
    
    // Track if we found any opportunities
    let foundOpportunities = false;
    
    // Add long opportunities
    if (oppsToDisplay.long.length > 0) {
        foundOpportunities = true;
        // Limit how many we show
        const displayLimit = appState.opportunityDisplayMode === "current" ? 5 : 15;
        
        oppsToDisplay.long.slice(0, displayLimit).forEach(opp => {
            const strength = `${parseFloat(opp.opportunity_strength).toFixed(1)}%`;
            const pricePct = `${parseFloat(opp.price_metric).toFixed(2)}%`;
            const priceStyle = parseFloat(pricePct) > 0 ? 'green' : 'red';
            
            panelHtml += `
                <tr>
                    <td class="cyan">LONG</td>
                    <td class="green">${opp.symbol}</td>
                    <td style="text-align: right;">${strength}</td>
                    <td class="${priceStyle}" style="text-align: right;">${pricePct}</td>
                    ${appState.opportunityDisplayMode === "historical" ? `<td>${opp.detection_time || 'N/A'}</td>` : ''}
                </tr>
            `;
        });
    }
    
    // Add a separator if we have both long and short opportunities
    if (oppsToDisplay.long.length > 0 && oppsToDisplay.short.length > 0) {
        const cols = appState.opportunityDisplayMode === "current" ? 4 : 5;
        panelHtml += `<tr><td colspan="${cols}">&nbsp;</td></tr>`;
    }
    
    // Add short opportunities
    if (oppsToDisplay.short.length > 0) {
        foundOpportunities = true;
        // Limit how many we show
        const displayLimit = appState.opportunityDisplayMode === "current" ? 5 : 15;
        
        oppsToDisplay.short.slice(0, displayLimit).forEach(opp => {
            const strength = `${parseFloat(opp.opportunity_strength).toFixed(1)}%`;
            const pricePct = `${parseFloat(opp.price_metric).toFixed(2)}%`;
            const priceStyle = parseFloat(pricePct) > 0 ? 'green' : 'red';
            
            panelHtml += `
                <tr>
                    <td class="cyan">SHORT</td>
                    <td class="green">${opp.symbol}</td>
                    <td style="text-align: right;">${strength}</td>
                    <td class="${priceStyle}" style="text-align: right;">${pricePct}</td>
                    ${appState.opportunityDisplayMode === "historical" ? `<td>${opp.detection_time || 'N/A'}</td>` : ''}
                </tr>
            `;
        });
    }
    
    // If no opportunities were found
    if (!foundOpportunities) {
        const cols = appState.opportunityDisplayMode === "current" ? 4 : 5;
        panelHtml += `
            <tr>
                <td>INFO</td>
                <td>No opportunities</td>
                <td style="text-align: right;">N/A</td>
                <td style="text-align: right;">N/A</td>
                ${appState.opportunityDisplayMode === "historical" ? '<td>N/A</td>' : ''}
            </tr>
        `;
    }
    
    // Close the table
    panelHtml += `
            </tbody>
        </table>
    `;
    
    // Update the container
    opportunitiesPanel.html(panelHtml);
}

// Update the backtest results UI
function updateBacktestUI() {
    const backtestPanel = $('#backtest-panel');
    
    // Skip if no backtest results
    if (!appState.backtestSummary) {
        backtestPanel.html('<div class="panel-title">Backtest Results</div><div class="loading-message">No backtest results</div>');
        return;
    }
    
    // Build HTML for backtest results
    let backtestHtml = `
        <div class="panel-title">Backtest Results</div>
    `;
    
    // Summary section
    backtestHtml += `
        <div class="backtest-summary">
            <h3>Summary Statistics</h3>
            <table class="opportunities-table">
                <tr>
                    <th>Strategy</th>
                    <th>Trades</th>
                    <th>Win Rate</th>
                    <th>Total PnL</th>
                    <th>Avg PnL</th>
                </tr>
    `;
    
    // Add long summary
    if (appState.backtestSummary.long) {
        const long = appState.backtestSummary.long;
        backtestHtml += `
            <tr>
                <td>Long</td>
                <td>${long.total_trades}</td>
                <td>${parseFloat(long.win_rate).toFixed(2)}%</td>
                <td class="${parseFloat(long.total_pnl) >= 0 ? 'green' : 'red'}">${parseFloat(long.total_pnl).toFixed(2)}%</td>
                <td class="${parseFloat(long.average_pnl) >= 0 ? 'green' : 'red'}">${parseFloat(long.average_pnl).toFixed(2)}%</td>
            </tr>
        `;
    }
    
    // Add short summary
    if (appState.backtestSummary.short) {
        const short = appState.backtestSummary.short;
        backtestHtml += `
            <tr>
                <td>Short</td>
                <td>${short.total_trades}</td>
                <td>${parseFloat(short.win_rate).toFixed(2)}%</td>
                <td class="${parseFloat(short.total_pnl) >= 0 ? 'green' : 'red'}">${parseFloat(short.total_pnl).toFixed(2)}%</td>
                <td class="${parseFloat(short.average_pnl) >= 0 ? 'green' : 'red'}">${parseFloat(short.average_pnl).toFixed(2)}%</td>
            </tr>
        `;
    }
    
    // Add combined summary
    if (appState.backtestSummary.combined) {
        const combined = appState.backtestSummary.combined;
        backtestHtml += `
            <tr>
                <td><b>Combined</b></td>
                <td><b>${combined.total_trades}</b></td>
                <td><b>${parseFloat(combined.win_rate).toFixed(2)}%</b></td>
                <td class="${parseFloat(combined.total_pnl) >= 0 ? 'green' : 'red'}"><b>${parseFloat(combined.total_pnl).toFixed(2)}%</b></td>
                <td class="${parseFloat(combined.average_pnl) >= 0 ? 'green' : 'red'}"><b>${parseFloat(combined.average_pnl).toFixed(2)}%</b></td>
            </tr>
        `;
    }
    
    backtestHtml += `
            </table>
        </div>
    `;
    
    // Update container
    backtestPanel.html(backtestHtml);
}

// Show loading indicator
function showLoadingIndicator(message = "Loading...") {
    $('#loading-text').text(message);
    $('#loading-indicator').show();
}

// Hide loading indicator
function hideLoadingIndicator() {
    $('#loading-indicator').hide();
}

// Show status message
function showStatusMessage(message, type = "info") {
    const statusMessage = $('#status-message');
    
    // Set message and style based on type
    statusMessage.text(message);
    statusMessage.css('borderColor', type === "error" ? "#cc0000" : type === "success" ? "#4e9a06" : "#3465a4");
    
    // Show the message
    statusMessage.show();
    
    // Hide after 3 seconds
    setTimeout(() => {
        statusMessage.hide();
    }, 3000);
}
