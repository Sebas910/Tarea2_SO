#!/usr/bin/env bash
# Pruebas automaticas del torneo (requiere: make asan). Ejecutar desde la raiz:  make test
cd "$(dirname "$0")/.." || exit 1
BIN=./torneo_asan
OK=0; FAIL=0
ok()   { echo "  [OK]   $1"; OK=$((OK+1)); }
fallo(){ echo "  [FALLO] $1"; FAIL=$((FAIL+1)); }
TMP=$(mktemp -d); trap 'rm -rf "$TMP"' EXIT

# Concurrencia maxima de mesas: +1 en "INICIO DE LA PARTIDA", -1 en cada evento de fin.
maxmesas() {
  awk '/INICIO DE LA PARTIDA/{a++; if(a>m)m=a}
       /\] (VICTORIA|EMPATE|FALLO|PARTIDA FINALIZADA)/{a--}
       END{print m+0}' "$1"
}

echo "1) Limite de mesas (N=24, M=3) con ASan/UBSan/LeakSanitizer"
timeout 300 $BIN 24 3 > "$TMP/o1" 2> "$TMP/e1"; rc=$?
[ $rc -eq 0 ] && ok "termina con codigo 0" || fallo "codigo de salida $rc"
[ ! -s "$TMP/e1" ] && ok "sin errores de memoria ni leaks" || { fallo "reportes de sanitizer:"; head -5 "$TMP/e1"; }
mx=$(maxmesas "$TMP/o1"); [ "$mx" -le 3 ] && ok "maximo de mesas simultaneas = $mx (<= 3)" || fallo "se excedio M: $mx"
np=$(grep -c "INICIO DE LA PARTIDA" "$TMP/o1"); [ "$np" -eq 12 ] && ok "se jugaron las 12 partidas esperadas" || fallo "partidas jugadas: $np (esperadas 12)"

echo "2) SIGINT (Ctrl+C) durante el torneo (N=30, M=2)"
$BIN 30 2 > "$TMP/o2" 2> "$TMP/e2" & pid=$!
sleep 2; kill -INT $pid
for _ in $(seq 1 100); do kill -0 $pid 2>/dev/null || break; sleep 0.1; done
if kill -0 $pid 2>/dev/null; then kill -9 $pid; fallo "no termino en 10 s tras SIGINT"; else ok "termina en menos de 10 s"; fi
wait $pid 2>/dev/null; rc=$?
[ $rc -eq 0 ] && ok "codigo de salida 0" || fallo "codigo de salida $rc"
[ ! -s "$TMP/e2" ] && ok "sin errores de memoria ni leaks tras SIGINT" || { fallo "reportes de sanitizer:"; head -5 "$TMP/e2"; }

echo "3) Caida de jugadores (config PROB_FALLO_MILESIMAS=500)"
timeout 300 $BIN 8 2 configs/fallos.txt > "$TMP/o3" 2> "$TMP/e3"; rc=$?
[ $rc -eq 0 ] && ok "termina con codigo 0" || fallo "codigo de salida $rc"
grep -qE "ABANDONO|ABORTADA" "$TMP/o3" && ok "el Juez detecto el abandono y cerro la partida" || fallo "no se detecto ningun abandono (puede ser azar: repetir)"
[ ! -s "$TMP/e3" ] && ok "sin errores de memoria" || fallo "reportes de sanitizer"

echo "4) N impar (N=5, M=2): el cliente sobrante no debe colgarse"
timeout 120 $BIN 5 2 > "$TMP/o4" 2> "$TMP/e4"; rc=$?
[ $rc -eq 0 ] && grep -q "queda sin pareja" "$TMP/o4" && ok "cliente sin pareja manejado" || fallo "problema con N impar (rc=$rc)"

echo; echo "Resultado: $OK OK, $FAIL fallos"; [ $FAIL -eq 0 ]
