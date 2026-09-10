# Incorporación a TaxOps — Lorena

**Para:** Lorena (`lorena`)  
**Rol:** Preparadora (preparer)  
**Fecha:** septiembre 2026

Entregue esta hoja la primera vez que inicie sesión. La contraseña temporal está abajo — **cámbiela de inmediato** cuando TaxOps lo pida.

---

## 1. Abrir TaxOps

| | |
|---|---|
| **URL** | http://192.168.1.173:5000 |
| **O** | http://taxlog:5000 (después de configurar la workstation) |
| **Desktop** | Shortcut “Tax Log” (si ya corrió el setup) |

Si la página no carga: confirme los network drives (sobre todo **T:**) con `\\Xcel-server\taxops\SETUP_WORKSTATION.bat`, o pregunte a Moisés / admin.

---

## 2. Su acceso

| Nombre | Usuario | Contraseña temporal |
|--------|---------|---------------------|
| Lorena | `lorena` | `temp@92` |

1. Inicie sesión con `lorena` y **`temp@92`**.
2. TaxOps pedirá una **contraseña nueva** — elija una que solo usted conozca (y que cumpla la longitud en pantalla).
3. Puede aparecer una pantalla de **Bienvenida** una sola vez — avance.
4. Opcional: **Tour guiado** desde el ícono de ayuda en la barra superior.

No comparta `temp@92` después de cambiarla. Si se bloquea, pida a un admin que restablezca la contraseña.

---

## 3. Qué puede hacer (preparadora)

Está configurada como **preparer**: trabajo diario de impuestos (no herramientas solo de admin).

### Barra principal
- **Dashboard** — buscar clientes / declaraciones, filtros, estatus  
- **Bookkeeping** — cola de contabilidad mensual  
- **New Intake** — hoja de entrada (walk-in / cliente que regresa)

### Menú Daily (la mayor parte del día)
- **Pickup Queue** — clientes listos en mostrador  
- **E-File Queue** — listos para transmitir / lote  
- **Work Orders** — solicitudes sin declaración  
- **Now Serving** — turnos del lobby (también el botón **# Now Serving** arriba). Call Next, enviar a la otra ventana; el panel se queda abierto mientras trabaja  
- **Extension Queue** — prórrogas  
- **Docs not scanned** — terminar escaneos omitidos en intake  

### En una declaración
- Abra un cliente desde el Dashboard → documentos, notas, docs faltantes, espacio **Prep**, cambios de estatus, escanear/subir  

### Occasional (cuando haga falta)
- **Compliance Tracker** / **Filing Periods** — impuestos sobre ventas y licencias  
- Lotes, comparaciones, etc. según su rol  

**Solo admin (usted no verá):** Email Inbox, Email Campaigns, Staff Accounts, Audit log, Sender Rules, reinicio del día en Now Serving, etc.

---

## 4. Lista del primer día

- [ ] Abrir http://192.168.1.173:5000 e iniciar sesión  
- [ ] Cambiar la contraseña de `temp@92`  
- [ ] Terminar Bienvenida / orientación si aparece  
- [ ] Buscar un cliente conocido en el Dashboard  
- [ ] Abrir **Daily → Now Serving** (o el botón del encabezado) y confirmar que el panel abre  
- [ ] Abrir **Pickup Queue** y **E-File Queue** una vez para ubicarlos  
- [ ] Preguntar a un compañero qué estatus usan mientras trabajan en Drake  

---

## 5. Tips rápidos

- **Privacy** en el encabezado oculta nombres cuando hay un cliente en su desk.  
- Controles de **tamaño de texto** junto a Privacy.  
- En **Now Serving** los números son simples (1, 2, 3…). La TV/kiosco del lobby anuncia en inglés y español.  
- ¿Dudas? Ícono de ayuda → Tour guiado, o **Ops Runbook** en el pie, o pregunte a Moisés / admin.

---

## 6. Para el admin (Moisés)

| Usuario | Nombre | Rol | Notas |
|---------|--------|-----|--------|
| `lorena` | Lorena | preparer | `must_change_password` debe estar activo hasta el primer cambio exitoso |

Contraseña temporal (solo este handoff): **`temp@92`**

Después de que cambie la contraseña, confirme Dashboard + Now Serving. Si falla el login, restablezca desde **Occasional → Staff Accounts** y reactive “must change password.”
