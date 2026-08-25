# R1 — PROFILE disagreement report (pre-write)

_Generated: 2026-08-13T20:30:05Z · strict `^25\d{4}$` · invoice→bare→client only_

Where TaxOps and Drake both hold a value and they differ — **no writes**.

| Kind | n |
|---|---:|
| `PROFILE_TRUNCATION` | 50 |
| `PROFILE_CONTAMINATION` | 13 |
| `PROFILE_STALE` | 216 |
| **Total disagreements** | **279** |
| Distinct contamination clients | **6** |

## Compared (both sides non-empty)

| Field | Compared | Agree |
|---|---:|---:|
| `address` | 674 | 605 |
| `spouse_first_name` | 219 | 185 |
| `spouse_last_name` | 161 | 63 |
| `taxpayer_dob` | 641 | 569 |
| `taxpayer_email` | 13 | 7 |

## PROFILE_CONTAMINATION (13 findings / 6 clients)

TaxOps holds a different real person. Escalate — do not auto-fix.

| Client | Bare | Field | TaxOps | Drake | Detail |
|---|---|---|---|---|---|
| `1670` | `888` | `spouse_last_name` | DURAN | HUERTA | taxops_spouse_matches_client:258 |
| `1670` | `888` | `spouse_first_name` | GUADALUPE | EVELYN | taxops_spouse_matches_client:258 |
| `1670` | `888` | `spouse_last_name` | DURAN | HUERTA | taxops_spouse_matches_client:258 |
| `1670` | `888` | `spouse_first_name` | GUADALUPE | EVELYN | taxops_spouse_matches_client:258 |
| `1583` | `1028` | `spouse_last_name` | OLEA | GAMBOA GARCIA | taxops_spouse_elsewhere:[('spouses', 747)] |
| `1583` | `1028` | `spouse_first_name` | VERONICA | MERCEDES | taxops_spouse_elsewhere:[('spouses', 747)] |
| `902` | `334` | `spouse_last_name` | GUADALUPE MELENDREZ | ROSAS | taxops_spouse_matches_client:578 |
| `902` | `334` | `spouse_first_name` | M | KARLA | taxops_spouse_matches_client:578 |
| `327` | `910` | `spouse_last_name` | GALLARDO | YANEZ | taxops_spouse_matches_client:159 |
| `327` | `910` | `spouse_first_name` | MARTHA | HERLINDA | taxops_spouse_matches_client:159 |
| `237` | `275` | `spouse_last_name` | A | FLORES | taxops_spouse_elsewhere:[('spouse_cols', 1087), (' |
| `878` | `454` | `spouse_last_name` | RODAS | ROMERO SERRANO | taxops_spouse_matches_client:433 |
| `878` | `454` | `spouse_first_name` | CARIDAD | MARIO | taxops_spouse_matches_client:433 |

## PROFILE_TRUNCATION (50)

| Client | Field | TaxOps | Drake |
|---|---|---|---|
| `1551` | `spouse_last_name` | CASSIN | CASSINERIO |
| `129` | `spouse_last_name` | A | CACERES |
| `344` | `spouse_first_name` | ANA | ANA ROSA |
| `1480` | `spouse_last_name` | RODRIGUEZ | RODRIGUEZ PENA |
| `716` | `spouse_last_name` | ZUNIGA NAVARR | ZUNIGA NAVARRO |
| `716` | `spouse_last_name` | ZUNIGA NAVARR | ZUNIGA NAVARRO |
| `1520` | `spouse_last_name` | ALONSO GONZALE | ALONSO GONZALEZ |
| `1512` | `spouse_last_name` | LEYVA VI | LEYVA VILLALVA |
| `1488` | `spouse_last_name` | FLORES D ROD | FLORES D RODRIG |
| `1486` | `spouse_last_name` | H | RIZKALLAH |
| `1486` | `spouse_first_name` | MUNA | MUNA H |
| `578` | `spouse_last_name` | P | HERNANDEZ P |
| `926` | `spouse_last_name` | HERN | HERNANDEZ C |
| `384` | `spouse_last_name` | L | GONZALES |
| `1681` | `spouse_last_name` | RUIZ DE PERE | RUIZ DE PEREZ |
| `1747` | `spouse_last_name` | AGUILA | AGUILAR |
| `1727` | `spouse_first_name` | SHAYMAA | SHAYMAA BASEL M |
| `1608` | `spouse_last_name` | GONZALEZ TINO | GONZALEZ-TINOCO |
| `617` | `spouse_first_name` | CAMELIA | CAMELIA C |
| `512` | `spouse_last_name` | A | KAVANAGH |
| `512` | `spouse_last_name` | A | KAVANAGH |
| `535` | `spouse_last_name` | D | LEDESMA |
| `1086` | `spouse_last_name` | FERNANDE | FERNANDEZ |
| `894` | `spouse_last_name` | G GUERRERO Q | GUERRERO QUIROZ |
| `894` | `spouse_last_name` | G GUERRERO Q | GUERRERO QUIROZ |

## PROFILE_STALE sample (25 / 216)

| Client | Field | TaxOps | Drake |
|---|---|---|---|
| `451` | `address` | 1316 S HERBERT AVE | 19360 LEMAY STREET |
| `132` | `taxpayer_email` | richard_90047@hotmail.com | RPOLANCO@CALIFORNIALIVEWIRE.COM |
| `264` | `taxpayer_email` | fitznotfritz@protonmail.com | EERIELANECOLLECTIVE@GMAIL.COM |
| `113` | `address` | 16436 FRANCISQUITO AVE | P O BOX 2099 |
| `488` | `taxpayer_email` | karprincic77@gmail.com | KARLPRINCIC77@GMAIL.COM |
| `172` | `address` | 3222 CEDAR AVE | 235 W 24TH ST |
| `709` | `taxpayer_email` | mikeaguilar.roots@gmail.com | MYMORGANICS774@GMAIL.COM |
| `1590` | `address` | 1833 S NORMANDIE AVE | 69876 PAPAYA LN |
| `490` | `taxpayer_email` | jnhmechanical@gmail.com | JNHMECHANICALINC@GMAIL.COM |
| `247` | `spouse_last_name` | M | DIAZ |
| `551` | `spouse_last_name` | NUBIA | MEDRANO |
| `551` | `spouse_first_name` | MEDRANO | NUBIA |
| `38` | `spouse_last_name` | M KDEISS | KDEISS |
| `783` | `taxpayer_dob` | 1999-09-15 | 02/04/1971 |
| `1540` | `spouse_last_name` | A EL SAID | EL-SAID |
| `1566` | `spouse_last_name` | A ALVARADO PALAC | ALVARADO PALACIOS |
| `742` | `spouse_last_name` | D | ESTEVEZ |
| `423` | `taxpayer_dob` | 1944-04-15 | 10/18/1978 |
| `423` | `address` | 2670 RANGE RD | 7511 CYPRESS AVE |
| `880` | `spouse_last_name` | M | PORTALES |
| `1683` | `taxpayer_dob` | 2004-03-07 | 07/28/1973 |
| `7` | `taxpayer_dob` | 1999-05-25 | 12/06/1958 |
| `7` | `address` | 16165 VETERANS WAY | 9320 COSGROVE STREET |
| `769` | `address` | 3027 FRUITLAND AVE | 2394 WOODS AVE |
| `1627` | `taxpayer_dob` | 1987-10-20 | 05/27/1961 |

Prior 3/22 sample rate was too thin. This is the full strict-invoice disagreement census.

Machine: `T:\audit\investigation\R1-disagreement-report.json`
