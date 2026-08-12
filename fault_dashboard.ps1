# ========================================================
# GOSSIP PROTOCOL - FAULT INJECTION DASHBOARD
# ========================================================
# Assicurati che la rete di Docker Compose si chiami cosi
# (abbiamo esplicitamente nominato la rete 'gossip-net' nel docker-compose.yml)
$networkName = "gossip-net"

while ($true) {
    Clear-Host
    Write-Host "=========================================" -ForegroundColor Cyan
    Write-Host "       FAULT INJECTION DASHBOARD         " -ForegroundColor Yellow
    Write-Host "=========================================" -ForegroundColor Cyan
    Write-Host "1. [INFO]  Mostra lo stato di tutti i Nodi"
    Write-Host "2. [CRASH] Spegni un Nodo (Simula Crash Hardware)"
    Write-Host "3. [START] Riaccendi un Nodo (Simula Recovery)"
    Write-Host "4. [DROP]  Isola un Nodo (Simula Network Partition)"
    Write-Host "5. [JOIN]  Riconnetti un Nodo (Simula Rete Ripristinata)"
    Write-Host "0. [EXIT]  Esci dalla Dashboard"
    Write-Host "-----------------------------------------"

    $choice = Read-Host "Seleziona uno scenario (0-5)"

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
        "4" {
            $node = Read-Host "`nInserisci il nome del nodo da isolare (es. gossip-node3)"
            Write-Host "Iniezione partizione di rete in corso..." -ForegroundColor Yellow
            docker network disconnect $networkName $node
            Write-Host "[DROP] Network Partition simulata: Nodo $node isolato. Il processo e' attivo ma irraggiungibile." -ForegroundColor DarkYellow
        }
        "5" {
            $node = Read-Host "`nInserisci il nome del nodo da riconnettere (es. gossip-node3)"
            Write-Host "Ripristino rete in corso..." -ForegroundColor Yellow
            docker network connect $networkName $node
            Write-Host "[JOIN] Rete ripristinata per il nodo $node." -ForegroundColor Green
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