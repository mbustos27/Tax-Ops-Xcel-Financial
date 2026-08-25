# R1 Phase 4 — FS cross-check on `TAXPAYER.csv`

_Generated: 2026-08-13T22:46:00Z · sha `33bcda025ef8a4db1e7b189da13dbebbd7c7ea170d2f9c5f5220a6ca6cf529c8` · joinable=707_

## Assertions (joinable only)

| Assertion | n | Extrapolate ×1493 clients |
|---|---:|---:|
| Drake Single (1) + TaxOps has spouse | **15** | ~32 |
| Drake MFJ (2) + TaxOps no spouse | **48** | ~101 |

FS on joinable: 1=284, 2=232, 3=4, 4=158, 5=1

### Sample Single+spouse

| Client | Bare | Invoice | Name | Drake spouse |
|---|---|---|---|---|
| `1079` | `1256` | `251256` | HERRERA, BERTHA |  |
| `940` | `503` | `250503` | SANDOVAL, ADRIANA |  |
| `498` | `168` | `250168` | LOPEZ, JUAN |  |
| `1326` | `1258` | `251258` | CHAVARRIA, ALFREDO |  |
| `527` | `1080` | `251080` | LAZO, ANNABEL |  |
| `143` | `364` | `250364` | CARDONA, HECTOR |  |
| `352` | `394` | `250394` | VEGA, DIANA |  |
| `994` | `1087` | `251087` | LEMUS, EVELYN |  |
| `644` | `855` | `250855` | MENDEZ, SERGIO |  |
| `672` | `771` | `250771` | MONTEJO GONZALEZ, MIGUEL |  |
| `523` | `1143` | `251143` | LANDEROS, AURELIO |  |
| `747` | `1059` | `251059` | OLEA, ALEJANDRA |  |

### Sample MFJ+no-spouse

| Client | Bare | Invoice | Name | Drake spouse |
|---|---|---|---|---|
| `1542` | `702` | `250702` | HUERTA, JACOB | STEPHANIE ALANIS |
| `1534` | `427` | `250427` | JIMENEZ ESCOTO, RAUL | VIRGINIA MORENO BERNAL |
| `432` | `479` | `250479` | ARROYO HERNANDEZ, HUMBERTO | MARIA HERNANDEZ |
| `1683` | `458` | `250458` | BRAVO, VINCENT | ELISA BRAVO |
| `7` | `181` | `250181` | VELASQUEZ, CARLOS G | MARIA E VELASQUEZ |
| `2128` | `1253` | `251253` | LOPEZ, AURELIO | LUCERO LOPEZ |
| `957` | `1043` | `251043` | RAMIREZ, ROBERT | MARIA BALBOA |
| `1627` | `175` | `250175` | MUNGUIA, FEDERICO | MARIBEL MUNOZ |
| `307` | `347` | `250347` | LIMA-MARROQUIN, ALFONSO | T TORRES |
| `690` | `745` | `250745` | YANEZ, ANGEL | MARGARITA MENDOZA |
| `1448` | `188` | `250188` | GOMEZ, CAROLINA | JUAN GOMEZ |
| `643` | `698` | `250698` | MICLAT JR, ALEX | ELIZABETH MICLAT |

Machine: `T:\audit\investigation\R1-phase4-fs-taxpayer.json`
