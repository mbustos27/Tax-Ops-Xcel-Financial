# Human review — Drake log collisions

_Generated: 2026-08-12T22:46:59Z · cut=772 · 19 cases / 40 tasks_

## Hold

- Do not split Tax Log genuine_reuse keys in TaxOps — Drake corrections only.

## Lucy packet fixes

- QUINTANA 1103 pulled MOVE→REVIEW (Jr/L; prefer 1103 via Kayla)
- do_after on MOVE/CLAIM landing on bares still being cleared
- REVIEW lists all Log alternatives (DURAN 640+653)
- 1038 MONTIEL MINT→CLAIM vacant after BARAJAS MOVE
- 589 DENISE Log transpose note; 141 MOVEs only in priority section

## Verbs

| Verb | n | Meaning |
|---|---:|---|
| `MOVE` | 28 | In Drake, change this client's log/invoice FROM wrong_bare TO to_bare. |
| `KEEP` | 2 | Leave on wrong_bare — canonical Log household for that number. |
| `CLAIM` | 1 | After clearers leave, assign this client onto the now-vacant collision bare. |
| `MINT` | 2 | Assign a new unused log number (bare already occupied by keeper). |
| `REVIEW` | 7 | Ambiguous or band2 destination — confirm with preparer; see alternatives. |

## Work order

1. Priority case bare 141 (I7)
2. MOVE checklist (excludes 141 — already above)
3. KEEP + MINT / CLAIM (CLAIM after its do_after clearers)
4. REVIEW last (suffix / band2 ambiguity)

## 1. Priority — bare `141` (I7)

Canonical on Log: `PEREZ & GARCIA VILLAREAL, ANDRES & IRAZU` (row 10, LOGOUT)

| Verb | Claimant | To # | Log name | Status |
|---|---|---|---|---|
| `KEEP` | PEREZ, ANDRES | `141` | PEREZ & GARCIA VILLAREAL, ANDRES & IRAZU | LOGOUT |
| `MOVE` | VILLAREAL, RICHARD | `143` | VILLAREAL, RICHARD P | LOGOUT |
| `MINT` | GOYTIA, SANDRA | `—` | — | — |

_VILLAREAL MOVE lives only here — not repeated in the MOVE checklist below._

## 2. MOVE checklist (27)

Change Drake log/invoice **from → to**. Honor **Do after** before checking off.

| ☐ | From # | Claimant | To # | Log name | Status | Do after |
|---|---|---|---|---|---|---|
| ☐ | `203` | LAZO, BREANNA | `680` | LAZO, BREANNA L | LOGOUT | — |
| ☐ | `203` | NUNO, MARIBEL | `199` | NUNO, MARIBEL M | LOGOUT | — |
| ☐ | `206` | BRICENO, JUAN | `217` | BRICENO, JUAN | LOGOUT | — |
| ☐ | `206` | DELGADO, TIFFANY | `202` | DELGADO, TIFFANY | LOGOUT | — |
| ☐ | `207` | COLLAZO, MAYRA | `203` | COLLAZO, MIGUEL & MAYRA | LOGOUT | LAZO, BREANNA off `203`, NUNO, MARIBEL off `203` |
| ☐ | `207` | RAMOS, VALERIE | `218` | RAMOS, VALERIE | LOGOUT | — |
| ☐ | `247` | BRAVO, VICTORIA | `242` | BRAVO, VICTORIA | LOGOUT | — |
| ☐ | `335` | GALLEGOS, ALEJANDRO | `334` | GALLEGOS, ALEJANDRO & MARIA D | LOGOUT | — |
| ☐ | `335` | ROJAS, LUIS | `333` | ROJAS, LUIS & GUADALUPE MELENDREZ | LOGOUT | — |
| ☐ | `339` | AGUILAR, AMBER | `341` | AGUILAR, AMBER | LOGOUT | — |
| ☐ | `339` | LOPEZ, SAUL | `338` | LOPEZ, SAUL & YESENIA | LOGOUT | — |
| ☐ | `353` | AVILA, LUCY | `349` | AVILA, LUCY & EDUARDO | LOGOUT | — |
| ☐ | `353` | SALAMA, ENGIE | `350` | SALAMA, ENGIE | LOGOUT | — |
| ☐ | `426` | HERNANDEZ, AMADO | `402` | HERNANDEZ, AMADO & ARMIDA | LOGOUT | — |
| ☐ | `589` | SANTIAGO, EUNICE | `588` | SANTIAGO, EUNICE | LOGOUT | — |
| ☐ | `634` | HERREJON, NICHOLAS | `631` | HERREJON, NICHOLAS K | LOGOUT | — |
| ☐ | `787` | VICENTE, JOSE | `775` | VICENTE, JOSE | LOGOUT | — |
| ☐ | `862` | LUNA, JACINTO | `834` | LUNA, JACINTO & JUANA | LOGOUT | — |
| ☐ | `862` | MURILLO, JULIO | `835` | MURILLO, JULIO & LORETTA | LOGOUT | — |
| ☐ | `888` | HUERTA, JESUS | `855` | HUERTA, JESUS & EVELYN | LOGOUT | — |
| ☐ | `910` | YANEZ, ELEUTERIO | `862` | YANEZ, ELEUTERIO | LOGOUT | LUNA, JACINTO off `862`, MURILLO, JULIO off `862` |
| ☐ | `1038` | JORGE BARAJAS, ISRAEL | `933` | BARAJAS, ISRAEL JORGE | LOGOUT | — |
| ☐ | `1075` | LEDESMA, DAVID | `955` | LEDESMA, DAVID D & ALICIA D | LOGOUT | — |
| ☐ | `1075` | RODRIGUEZ, LEONOR | `954` | RODRIGUEZ, LEONOR | LOGOUT | — |
| ☐ | `1095` | GUTIERREZ, JOAO | `968` | GUTIERREZ, JOAO | LOGOUT | — |
| ☐ | `1095` | GUTIERREZ, MERCEDES | `967` | GUTIERREZ, MERCEDES | LOGOUT | — |
| ☐ | `1095` | RODRIGUEZ, JAVIER | `962` | RODRIGUEZ, JAVIER | LOGOUT | — |

## 3. KEEP / MINT / CLAIM (2 keep / 2 mint / 1 claim)

KEEP+MINT: confirm keeper, then mint. CLAIM: wait for Do after, then use vacant bare.

### Bare `141` — Log `PEREZ & GARCIA VILLAREAL, ANDRES & IRAZU`

| Verb | Claimant | To # | Notes |
|---|---|---|---|
| `KEEP` | PEREZ, ANDRES | `141` | stays on `141` |
| `MINT` | GOYTIA, SANDRA | `—` | assign **new** unused log # |

### Bare `247` — Log `VILLALTA, ROSALINDA V`

| Verb | Claimant | To # | Notes |
|---|---|---|---|
| `MINT` | PERFECT SMILE MANAGEMENT CORP | `—` | assign **new** unused log # |

### Bare `589` — Log `DENISE, SANTIAGO`

| Verb | Claimant | To # | Notes |
|---|---|---|---|
| `KEEP` | SANTIAGO, DENISE | `589` | Log stores transposed name DENISE, SANTIAGO -- correct Log to SANTIAGO, DENISE while editing (name-tier match will keep failing). |

### Bare `1038` — Log `(none)`

| Verb | Claimant | To # | Notes |
|---|---|---|---|
| `CLAIM` | MONTIEL FREYRE, SAMANTA | `1038` | Bare 1038 has no Log row; after JORGE BARAJAS MOVE to 933 it is unissued. Log Samanta at 1038 instead of minting a new number. Do after: JORGE BARAJAS, ISRAEL off `1038`. |

## 4. REVIEW before change (7)

Ambiguous destination or active band2. ★ = preferred when annotated.

| ☐ | From # | Claimant | Primary to # | Alternatives | Status | Notes |
|---|---|---|---|---|---|---|
| ☐ | `426` | HERNANDEZ, LYDIA | `304` | — | PROCESSING | — |
| ☐ | `634` | DAKAK, RAMI | `579` | — | PROCESSING | — |
| ☐ | `787` | CREATE YOUR HEALTH LLC | `597` | — | PROCESSING | — |
| ☐ | `888` | DURAN, AGUSTIN | `640` | `640` DURAN ROJAS, AGUSTIN & GUADALUPE (HOLD); `653` DURAN, AGUSTIN JR (HOLD) | HOLD | — |
| ☐ | `910` | GALLARDO, LORENZO | `647` | — | HOLD | — |
| ☐ | `1103` | QUINTANA, ROGELIO | `1103` | `464` QUINTANA, ROGELIO L (LOGOUT); `1103` QUINTANA, ROGELIO JR (REVIEW) ★ | REVIEW | Jr vs L ambiguity. W5 last4 5180 has spouse KAYLA -> favors QUINTANA, ROGELIO JR on 1103 (likely KEEP). Do not MOVE to 464 (ROGELIO L) without preparer confirm. |
| ☐ | `1103` | SANCHEZ ARCE, EUNICE | `1171` | — | PROCESSING | — |

## Case index (by wrong bare)

| Wrong # | Canonical on Log | Tasks |
|---|---|---|
| `141` | PEREZ & GARCIA VILLAREAL, ANDRES & IRAZU | `KEEP`, `MINT`, `MOVE` |
| `203` | COLLAZO, MIGUEL & MAYRA | `MOVE` |
| `206` | SERRATO, ROCHELLE M | `MOVE` |
| `207` | GONZALEZ, MARTHA | `MOVE` |
| `247` | VILLALTA, ROSALINDA V | `MINT`, `MOVE` |
| `335` | ROSAS, RICARDO & MARTHA | `MOVE` |
| `339` | GOMEZ, VIVIANA | `MOVE` |
| `353` | GONZALEZ, MARQUEZ | `MOVE` |
| `426` | — | `MOVE`, `REVIEW` |
| `589` | DENISE, SANTIAGO | `KEEP`, `MOVE` |
| `634` | SILVA, VINCENT & IRMA | `MOVE`, `REVIEW` |
| `787` | JACKSON, IRENE | `MOVE`, `REVIEW` |
| `862` | YANEZ, ELEUTERIO | `MOVE` |
| `888` | VALENCIA, NICOLE | `MOVE`, `REVIEW` |
| `910` | GAMBOA, ALEX | `MOVE`, `REVIEW` |
| `1038` | — | `CLAIM`, `MOVE` |
| `1075` | — | `MOVE` |
| `1095` | — | `MOVE` |
| `1103` | QUINTANA, ROGELIO JR | `REVIEW` |

---

Machine: `T:\audit\investigation\W-office-packet.json` · checklist CSV: `T:\audit\investigation\W-office-packet.csv`
