# ========================================================
# GOSSIP PROTOCOL - FAULT INJECTION DASHBOARD
# ========================================================
# NOTA: Le opzioni di Network Partition (disconnect/connect) sono state rimosse 
# in quanto la rimozione dell'interfaccia di rete in Docker manda in stato "sordo" 
# i socket UDP (stateless) in ascolto universale su 0.0.0.0.
# Per simulare correttamente fault in questo progetto UDP, usa le opzioni CRASH e START,
# che forzano il demone a ricreare i socket in modo pulito.

while ($true) {
    Clear-Host
    Write-Host "=========================================" -ForegroundColor Cyan
    Write-Host "       FAULT INJECTION DASHBOARD         " -ForegroundColor Yellow
    Write-Host "=========================================" -ForegroundColor Cyan
    Write-Host "1. [INFO]  Mostra lo stato di tutti i Nodi"
    Write-Host "2. [CRASH] Spegni un Nodo (Simula Crash Hardware)"
    Write-Host "3. [START] Riaccendi un Nodo (Simula Recovery)"
    Write-Host "0. [EXIT]  Esci dalla Dashboard"
    Write-Host "-----------------------------------------"

    $choice = Read-Host "Seleziona uno scenario (0-3)"

    switch ($choice) {
        "1" {
            Write-Host "`n--- STATO DEI CONTAINER ---" -ForegroundColor Cyan
            docker ps -a --format "table {{.Names}}`t{{.Status}}" | Select-String "node"
        }
        "2" {
            $node = Read-Host "`nInserisci il nome del nodo da spegnere (es. gossip-node3)"
            Write-Host "Iniezione guasto in corso..." -ForegroundColor Yellow
            docker stop $node
            Write-Host "[CRASH] Crash Hardware simulato: Nodo $node arrestato." -ForegroundColor Red
        }
        "3" {
            $node = Read-Host "`nInserisci il nome del nodo da riaccendere (es. gossip-node3)"
            Write-Host "Ripristino in corso..." -ForegroundColor Yellow
            docker start $node
            Write-Host "[START] Recovery simulato: Nodo $node avviato e in fase di riallineamento." -ForegroundColor Green
        }
        "0" {
            Write-Host "Uscita dalla Dashboard di Fault Injection..." -ForegroundColor Cyan
            break
        }
        default {
            Write-Host "Scelta non valida!" -ForegroundColor Red
        }
    }

    Write-Host "`nPremi INVIO per tornare al menu principale..."
    Pause | Out-Null
}
