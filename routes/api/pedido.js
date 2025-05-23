const Pedido = require("../../controllers/pedido_controller");
const Puntos_ganados_establecimiento = require("../../controllers/puntos_ganador_por_establecimiento_controller");
const passport = require("passport");

module.exports = function (router) {
  router.post(
    "/pedido",
    passport.authenticate("jwt", { session: false }),
    Pedido.create
  );
  router.post(
    "/updateMaster",
    passport.authenticate("jwt", { session: false }),
    Pedido.updateMaster
  );
  router.get(
    "/pedido",
    passport.authenticate("jwt", { session: false }),
    Pedido.getAll
  );

  router.get(
    "/pedido/:id",
    passport.authenticate("jwt", { session: false }),
    Pedido.get
  );

  router.get(
    "/pedido/detalle/:id",
    passport.authenticate("jwt", { session: false }),
    Pedido.getDetallado
  );

  // Información para construir la prefactura
  router.get(
    "/pedido/prefactura/:idPedido",
    passport.authenticate("jwt", { session: false }),
    Pedido.getPrefactura
  );

  router.get(
    "/pedido/validacion/volverPedir/:id",
    passport.authenticate("jwt", { session: false }),
   // Pedido.getPedidoProductos
   Pedido.getProductosVolver
  );

  router.get(
    "/pedido/editar/:id",
    passport.authenticate("jwt", { session: false }),
    Pedido.getInfoPedido
  );

  router.get(
    "/pedido_dist/:id",
    passport.authenticate("jwt", { session: false }),
    Pedido.pedidosPorDist
  );

  router.get(
    "/pedido/cancelar/horeca/:idPed",
    passport.authenticate("jwt", { session: false }),
    Pedido.cancelarPedidoPorHoreca
  );

  router.get(
    "/pedido/cancelar/distribuidor/:idPed",
    passport.authenticate("jwt", { session: false }),
    Pedido.cancelarPedidoPorDistribuidor
  );

  /**
   * Endpoint para pedidos por distribuidor usado en
   * componente de pedidos platadorma de un distribuidor
   */
  router.get(
    "/pedidos/detalle_pedidos_distribuidor/:IdDistribuidor",
    passport.authenticate("jwt", { session: false }),
    Pedido.AllPedidosPorDistribuidor
  );
  /**
   * Endpoint para pedidos SUGERIDOS por distribuidor usado en
   * componente de pedidos platadorma de un distribuidor
   */
  router.get(
    "/pedidos/detalle_pedidos_sugeridos_distribuidor/:IdDistribuidor",
    passport.authenticate("jwt", { session: false }),
    Pedido.AllPedidosSugeridosPorDistribuidor
  );

  /**
   * Endpoint para recuperar las calificaciones de un distribuidor
   */
  router.get(
    "/calificacion/:IdDistribuidor",
    passport.authenticate("jwt", { session: false }),
    Pedido.calificacionPorDistribuidor
  );

  router.put(
    "/pedido/:idPedido/:idTrabajador",
    passport.authenticate("jwt", { session: false }),
    Pedido.update
  );
  router.put(
    "/pedido_update/:idPedido",
    passport.authenticate("jwt", { session: false }),
    Pedido.pedido_update
  );
  router.put(
    "/editEstadoAdmin/:idPedido",
    passport.authenticate("jwt", { session: false }),
    Pedido.editEstadoAdmin
  );
  router.put(
    "/editarPedido/:idPedido",
    passport.authenticate("jwt", { session: false }),
    Pedido.updatePedido
  );

  router.delete(
    "/pedido/:id",
    passport.authenticate("jwt", { session: false }),
    Pedido.delete
  );

  router.get(
    "/pedido_horeca/:id",
    passport.authenticate("jwt", { session: false }),
    Pedido.getPedidoByPunto
  );

  router.get(
    "/pedido_punto_dist/:idPunto/:idDist",
    passport.authenticate("jwt", { session: false }),
    Pedido.getPedidosByPuntoDist
  );

  // Recupera los pedidos de un establecimiento
  router.get(
    "/pedido/establecimiento/:idHoreca",
    passport.authenticate("jwt", { session: false }),
    Pedido.getPedidosByEstablecimiento
  );

  router.get(
    "/pedido_punto_dist_count/:idPunto/:idDist",
    passport.authenticate("jwt", { session: false }),
    Pedido.getPedidosByPuntoDistCount
  );

  router.get(
    "/pedido_horeca_count/:id",
    passport.authenticate("jwt", { session: false }),
    Pedido.getPedidoCountByPunto
  );

  router.get(
    "/pedido/ultimoTracking/:idPedido",
    passport.authenticate("jwt", { session: false }),
    Pedido.ultimoEstadoPedido
  );

  // Recupera la fecha de aprobación de un pedido por el distribuidor
  router.get(
    "/pedido/fecha_aprobado_tracking/:idPedido",
    passport.authenticate("jwt", { session: false }),
    Pedido.trackingFechaAprobadoExterno
  );

  // Recupera la fecha de entrega de un pedido por el distribuidor
  router.get(
    "/pedido/fecha_entrega_tracking/:idPedido",
    passport.authenticate("jwt", { session: false }),
    Pedido.trackingFechaEntregado
  );

  router.get(
    "/productos_por_pedido/:id",
    passport.authenticate("jwt", { session: false })
  );

  router.get(
    "/productos_por_pedido/:id",
    passport.authenticate("jwt", { session: false }),
    Pedido.getProdsPorPedido
  );

  router.post(
    "/pedidos_por_punto_dist_fechas",
    passport.authenticate("jwt", { session: false }),
    Pedido.getPedidosPorPuntoDistFecha
  );

  // Servicio de historial de mensajes activos asociados a un punto de entrega
  router.get(
    "/mensajes/historial-activos/punto/:idPunto",
    passport.authenticate("jwt", { session: false }),
    Pedido.getChatsActivosByPunto
  );

  router.get(
    "/producto_por_pedido_por_punto/:id",
    passport.authenticate("jwt", { session: false }),
    Pedido.getProdutoPorPedidoPorPunto
  );

  // Servicio de historial de mensajes activos asociados a un distribuidor
  router.get(
    "/mensajes/historial-activos/distribuidor/:idDistribuidor",
    //passport.authenticate("jwt", { session: false }),
    Pedido.getChatsActivosByDistribuidor
  );

  // Recupera la cantidad de calificaciones de pedidos a un distribuidor y el total calificado (sumatoria)
  router.get(
    "/calificacion_total/distribuidor/:idDistribuidor",
    passport.authenticate("jwt", { session: false }),
    Pedido.getTotalCalificacionesDistribuidor
  );

  // Pedido de un producto en los ultimos 3 meses
  router.get(
    "/pedidos_ultimos_tres_meses/:idProducto",
    passport.authenticate("jwt", { session: false }),
    Pedido.getTotalPedidosProductosTresMeses
  );

  // Establecimeintos que pidieron el producto en los ultimos 3meses
  router.get(
    "/pedidos_establecimiento/:idProducto",
    passport.authenticate("jwt", { session: false }),
    Pedido.countPedidoProductoEstablecimiento
  );

  // Recupera el detalle de los puntos feat por punto de entrega
  router.get(
    "/puntos_feat/punto_entrega/:idPunto",
    passport.authenticate("jwt", { session: false }),
    Puntos_ganados_establecimiento.getDetallePuntosByPuntoEntrega
  );

  // Notifica a los trabajadores de los puntos de entrega sobre la creación de un producto
  router.get(
    "/notificar/trabajadores/punto/nuevo_producto/:id_dist",
    passport.authenticate("jwt", { session: false }),
    Pedido.notificarTrabPuntoNuevoProducto
  );

  // Notifica a los trabajadores de la organizacion sobre el pedido de uno de sus productos
  router.put(
    "/notificar/trabajadores/organizacion",
    passport.authenticate("jwt", { session: false }),
    Pedido.notificarTrabOrganizacion
  );
  // Notifica a los trabajadores de las horecas que tienen vinculado al distribuidor 
  router.put(
    "/notificar/trabajadores/horecasVinculado",
    passport.authenticate("jwt", { session: false }),
    Pedido.notificarTrabHoreca
  );

  // Notifica a los trabajadores de un distribuidor sobre la aprobación de uno de sus productos
  router.put(
    "/notificar/trabajadores/distribuidor/producto_aprobado",
    passport.authenticate("jwt", { session: false }),
    Pedido.notificarTrabDistribuidor
  );

  // METODO GENERICO para notificar a los trabajadores de un distribuidor sobre cualquier tipo de notificación
  router.put(
    "/notificar/trabajadores/distribuidor/generico",
    passport.authenticate("jwt", { session: false }),
    Pedido.notificarTrabDistGenerico
  );

  // METODO GENERICO para notificar a los trabajadores de un Horeca sobre cualquier tipo de notificación
  router.put(
    "/notificar/trabajadores/horeca/generico",
    passport.authenticate("jwt", { session: false }),
    Pedido.notificarTrabGenerico
  );

  // Recupera el total de tiempo de entrega de un pedido luego de finalizado y su detalle
  router.get(
    "/pedido/tiempo_entrega/:idPedido",
    passport.authenticate("jwt", { session: false }),
    Pedido.getTiempoEntregaPedido
  );

  // Top 10 productos mas vendidos distribuidor
  router.get(
    "/pedido/distribuidor/:idDist/fecha/:inicio/:fin",
    // passport.authenticate("jwt", { session: false }),
    Pedido.getTop10ProdDistMes
  );
   /**
     * ----------------------------------------------------------
     *          Nuevos API construccion APP distribuidor
     * ----------------------------------------------------------
     */
  router.get(
    "/detallePedidoAppDist/:idPed",
    Pedido.detallePedidoAppDist
  );
  router.get(
    "/detallePedidoAppDistIndicadores",
    Pedido.detallePedidoAppDistIndicadores
  );
  router.get(
    "/detallePedidoAppDistIndicadoresAll",
    Pedido.detallePedidoAppDistIndicadoresAll
  );
  router.get(
    "/valorMinimoPedido",
    Pedido.valorMinimoPedido
  );
  router.get(
    "/getGraficoBarsDistPedidos/:inicio/:fin",
    Pedido.getGraficoBarsDistPedidos
  );
  router.put(
    "/cancelarPedidoDist/:idPed",
    Pedido.cancelarPedidoDist
  );
  router.put(
    "/actPedidoDist/:idPed",
    Pedido.actPedidoDist
  );
  router.put(
    "/actualizarmasivoerrores/:idPed",
    Pedido.actualizarmasivoerrores
  );
  router.put(
    "/actulizarPedidoAppDist",
    Pedido.actulizarPedidoAppDist
  );
  router.put(
    "/actulizarPedidoAppDistNew",
    Pedido.actulizarPedidoNew
  );
  router.post(
    "/pedidoSugNewApi",
    //passport.authenticate("jwt", { session: false }),
    Pedido.pedidoSugNewApi
  );
  router.get(
    "/pedidoUtility/:id",
    Pedido.pedidoUtility
  );
    // Top 10 productos mas vendidos distribuidor
    router.get(
      "/top10AppDist",
      // passport.authenticate("jwt", { session: false }),
      Pedido.getTop10ProdDistMesApp
    );
    // Servicio de historial de mensajes activos asociados a un punto de entrega
    router.get(
      "/historialActivosNewAppDist/:idPunto/:page",
      Pedido.getChatsActivosByPuntoNewApp
    );
    router.get(
      "/reportePedidoPFALL/:horeca",
      Pedido.reportePedidoPFALL
    );
    router.put(
      "/actualizarmany/:idPed/:puntos",
      Pedido.actualizarmany
    );
};
