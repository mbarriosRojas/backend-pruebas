"use strict";

const moment = require("moment");
const momentTime = require('moment-timezone');
const _ = require('lodash');
const descuentosPunto = require("../models/productos_Descuentos_punto_model");
const Pedido = require("../models/pedido_model");
const Producto = require("../models/producto_model");
const puntoEntregaData = require("../models/punto_entrega_model");
const vinculacionesDistr = require("../models/distribuidores_vinculados_model");
const Puntos_ganados_establecimiento = require("../models/puntos_ganados_por_establecimiento_model");
const Pedido_Tracking = require("../models/pedido_tracking_model");
const Codigo_Generado = require("../models/codigos_descuento_generados_model");
const Mensaje = require("../models/mensajes_model");
const InformeUtils = require("../controllers/utils/informes_utils_controller");
const Notificaciones = require("../controllers/notificaciones_controller");
const Trabajador = require("../models/trabajador_model");
const horeca_user = require("../models/usuario_horeca_model");

const ObjectId = require("mongoose").Types.ObjectId;
const ReportePed = require("../models/reportePedidos_model");
const Distribuidor = require("../models/distribuidor_model");
const ProductosPorDistribuidor = require("../models/productos_por_distribuidor_model");
const BolsaPuntosFTOrganizaciones = require("../models/bolsa_puntos_organizacion_model");
const RedencionPuntosFTOrganizaciones = require("../models/redencion_puntos_ft_organizacion_model");
const DistribuidoresVinculados = require("../models/distribuidores_vinculados_model");
const Configuration_Model = require("../models/configuracions_model");
const jwt = require("jsonwebtoken");
const config = require("../config/database");
const Distribuidores_vinculados = require("../models/distribuidores_vinculados_model");
const mongoose = require('mongoose');
const e = require("cors");

exports.create = async function (req, res) {
  req.body.id_pedido = +new Date();
  req.body.tiempo_tracking_hora = new Date();

  await Pedido.create(req.body, async function (err, result) {
    if (req.body.puntos_ganados === 0) {
      for (let setProduct of req.body.productos) {
        setProduct.puntos_ft_unidad = 0;
      }
    }

    if (!err) {
      //Tracking inicial de pedido - Pendiente
      let puntosGanadosAfectados = [];
      let data = {
        pedido: result._id,
        estado_anterior: req.body.estado,
        estado_nuevo: req.body.estado,
        usuario: req.body.trabajador,
      };
      const tracking = await createPedidoTracking({ data: data });
      const historicoPed = await createHistoricoPedidosDistribuidor(
        req.body,
        result,
        tracking
      );
      let puntos_ganados;
      if (req.body.puntos_ganados > 0) {
        data = {
          punto_entrega: req.body.punto_entrega,
          pedido: result._id,
          puntos_ganados: req.body.puntos_ganados,
          movimiento: "Aplica",
          fecha: moment(),
        };
        puntos_ganados = await createPuntosGanados({ data: data });
      }
      if (req.body.codigo_descuento.length > 0) {
        for (let i in req.body.codigo_descuento) {
          await setCodigoGeneradoRedimido({
            _id: req.body.codigo_descuento[i],
            estado: "Redimido",
            idPedido: result._id,
          });
        }
        const listaPuntosGanados = await getPuntosGanados({
          punto_entrega: req.body.punto_entrega,
        });
        const meta = req.body.puntos_redimidos;
        let acumulados = 0;
        for (let j in listaPuntosGanados) {
          if (meta < acumulados) {
            acumulados = acumulados + listaPuntosGanados[j].puntos_ganados;
            puntosGanadosAfectados.push(listaPuntosGanados[j]._id);
            await applyRedimidos({
              _id: listaPuntosGanados[j]._id,
              estado: "Redimido",
            });
          }
        }
      }
      let mensajeArmado = "";
      if (req.body.estado !== "Sugerido") {
        mensajeArmado = `¡Se ha realizado un nuevo pedido con ID ${result.id_pedido}!`;
      } else {
        mensajeArmado = `¡Te han sugerido un pedido!`;
      }
      const trabajadores = await getTrabajadoresDelPunto({
        punto_entrega: result.punto_entrega,
        usuario_horeca: result.usuario_horeca,
      });
      await InformeUtils.extractIds(trabajadores);
      for (let i in trabajadores) {
        if (
          trabajadores[i].device_token !== "" &&
          trabajadores[i].device_token != null
        ) {
          await Notificaciones.sendNotification(
            {
              notification: {
                title: "Feat.",
                body: mensajeArmado,
                sound: "default",
              },
            },
            trabajadores[i].device_token
          );
        }
        await Notificaciones.registroNotificacion({
          tipo: "Actualización de pedido",
          mensaje: mensajeArmado,
          destinatario: trabajadores[i]._id,
        });
      }
      const mensaje_armado_distribuidor =
        "Haz recibido un nuevo pedido con ID: " + result.id_pedido;
      const trabajadores_distribuidor = await getTrabajadoresDelDistribuidor({
        distribuidor: result.distribuidor,
      });
      await InformeUtils.extractIds(trabajadores_distribuidor);
      for (let i in trabajadores_distribuidor) {
        await Notificaciones.registroNotificacion({
          tipo: "Actualización de pedido",
          mensaje: mensaje_armado_distribuidor,
          destinatario: trabajadores_distribuidor[i]._id,
        });
      }
      const resumen_res = {
        pedido: result,
        tracking: tracking,
        puntos: puntos_ganados ? puntos_ganados : "",
        puntosGanados: puntosGanadosAfectados ? puntosGanadosAfectados : "",
      };
      res.json(resumen_res);
    } else {
      res.status(400).json({
        success: false,
        message: "Error al registrar el Pedido",
        data: err,
      });
    }
  });
};
exports.updateMaster = async function (req, res) {

  // Actualiza PEDIDO
  let updatePed = await Pedido.findOneAndUpdate(
    { _id: req.body.idPedido },
    { $set: req.body.updatePedido },
    { new: true }
  );
  // Actualiza Traking
  let updateTraking = await Pedido_Tracking.findOneAndUpdate(
    { pedido: req.body.idPedido },
    { $set: req.body.updateTraking },
    { new: true }
  );
  // Actualiza Traking
  let updateReporte = await ReportePed.updateMany(
    { idPedido: req.body.idPedido },
    { $set: req.body.updateReporte },
    { new: true }
  );
  let notificaPedido = await notificaTrabDistEditado(req.body.idPedido);
  const resumen_res = {
    success: true,
  };

  res.json(resumen_res);
};
async function notificaTrabDistEditado(idPedido, idDist) {
  return new Promise((success) => {
    Pedido.getPedidoUpdate(idPedido, async function (err, result) {
      const vinculaciones = await DistribuidoresVinculados.find({
        distribuidor: result[0].distribuidor,
        punto_entrega: result[0].punto_entrega,
      });
      // Notifica a los trabajadores
      for (const trabajador of vinculaciones[0].vendedor) {
        await Notificaciones.registroNotificacion({
          tipo: "Nuevo producto en catalogo",
          mensaje:
            "El pedido " +
            result[0].id_pedido +
            " Ha sido editado por el establecimiento.",
          destinatario: trabajador,
        });
      }
      success(true);
    });
  });
}
/**
 *  Notifica a los trab. de una orga una novedad de su producto
 */
exports.notificarTrabPuntoNuevoProducto = async function (req, res) {
  try {
    // Data distribuidor
    const distribuidor = await Distribuidor.find({
      _id: req.params.id_dist,
    });

    // Get puntos de entrega vinculados al distribuidor
    const puntos_entregas = await getVinculacionesDistribuidor(
      req.params.id_dist
    );

    // Get trabajadores de cada punto
    let trabajadores = [];
    let trabajadores_ids = [];
    for (const punto of puntos_entregas) {
      const array_trabajadores = await Trabajador.find({
        puntos_entrega: punto,
      });
      trabajadores = trabajadores.concat(array_trabajadores);
    }

    // Guarda los Ids de los trabajadores
    for (const trabajador of trabajadores) {
      trabajadores_ids.push(trabajador._id);
    }

    // Elimina objetos/trabajadores repetidos para que no se notifique doble
    trabajadores_ids = uniq(trabajadores_ids);

    // Notifica a los trabajadores
    for (const trabajador of trabajadores_ids) {
      await Notificaciones.registroNotificacion({
        tipo: "Nuevo producto en catalogo",
        mensaje:
          "El distribuidor " +
          distribuidor[0].nombre +
          " ha añadido un nuevo producto a su catálogo.",
        destinatario: trabajador,
      });
    }
    // Salir exito
    res.status(201).json({
      success: true,
      message: "Los trabajadores han sido notificados con exito",
    });
    return;
  } catch (error) {
    res.status(400).json({
      success: false,
      message: "Error al notificar a los trabajadores",
      data: error,
    });
    return;
  }
};
function uniq(a) {
  let seen = {};
  return a.filter(function (item) {
    return seen.hasOwnProperty(item) ? false : (seen[item] = true);
  });
}
async function getVinculacionesDistribuidor(_id_dist) {
  const puntos_vinculados = [];
  const vinculaciones = await DistribuidoresVinculados.find({
    distribuidor: _id_dist,
    estado: "Aprobado",
  });
  for (const vinculacion of vinculaciones) {
    puntos_vinculados.push(vinculacion.punto_entrega);
  }
  return puntos_vinculados;
}

/**
 *  Notifica a los trab. de una orga una novedad de su producto
 */
exports.notificarTrabOrganizacion = async function (req, res) {
  notificaTrabajadorOrg(req.body);
};

async function notificaTrabajadorOrg(data_base_mensaje) {
  const trabajadores = await getTrabajadoresByOrganizacion({
    organizacion: new ObjectId(data_base_mensaje.organizacion),
  });
  for (let i in trabajadores) {
    await Notificaciones.registroNotificacion({
      tipo: "Novedad producto organización",
      mensaje: data_base_mensaje.mensaje,
      destinatario: trabajadores[i]._id,
    });
  }
}

async function getTrabajadoresByOrganizacion(obj) {
  return new Promise((success) => {
    Trabajador.getTrabajadoresByOrganizacion(obj, function (err, result) {
      success(result);
    });
  });
}

/**
 *  Notifica a los trab. de un distribuidor que se realizo un pedido de su producto
 */
exports.notificarTrabDistribuidor = async function (req, res) {
  notificaTrabDist(req.body);
};
async function notificaTrabDist(data_base_mensaje) {
  const trabajadores = await getTrabajadoresDelDist({
    distribuidor: new ObjectId(data_base_mensaje.distribuidor),
  });
  for (let i in trabajadores) {
    await Notificaciones.registroNotificacion({
      tipo: "Nuevo producto en catalogo",
      mensaje: data_base_mensaje.mensaje,
      destinatario: trabajadores[i]._id,
    });
  }
}
/**
 *  Notifica a los trab. de un distribuidor que se realizo un pedido de su producto
 */
exports.notificarTrabHoreca = async function (req, res) {
  notificaTrabHorecasAll(req.body);
};
async function notificaTrabHorecasAll(data_base_mensaje) {
  let trabajadores = [];
  const vinculaciones = await getAllVinculacionesDisHoreca(
    data_base_mensaje.distribuidor
  );
  if (vinculaciones) {
    let set = new Set(vinculaciones.map(JSON.stringify));
    let arrSinDuplicaciones = Array.from(set).map(JSON.parse);
    for (let user of arrSinDuplicaciones) {
      await Notificaciones.registroNotificacion({
        tipo: "Nuevo producto en catalogo",
        mensaje: data_base_mensaje.mensaje,
        destinatario: user._id,
      });
    }
  }
}
//Buscará todas los horecas vinculados
async function getAllVinculacionesDisHoreca(obj) {
  return new Promise((success) => {
    vinculacionesDistr.getInfoDataHoreca(obj, function (err, result) {
      success(result);
    });
  });
}
/**
 * METODO GENERICO para notificar a los trabajadores de un
 * distribuidor sobre cualquier tipo de notificación
 */
exports.notificarTrabDistGenerico = async function (req, res) {
  notificaTrabDistGenerico(req.body);
};
async function notificaTrabDistGenerico(data_base_mensaje) {
  const trabajadores = await getTrabajadoresDelDist({
    distribuidor: new ObjectId(data_base_mensaje.distribuidor),
  });
  for (let i in trabajadores) {
    await Notificaciones.registroNotificacion({
      tipo: data_base_mensaje.tipo,
      mensaje: data_base_mensaje.mensaje,
      destinatario: trabajadores[i]._id,
    });
  }
}
async function getTrabajadoresDelDist(obj) {
  return new Promise((success) => {
    Trabajador.getTrabajadoresByDistribuidorInfo(obj, function (err, result) {
      success(result);
    });
  });
}
async function getTrabajadoresHorecaVinculadoPunto(obj) {
  return new Promise((success) => {
    Trabajador.getTrabajadoresHorecaVinculadoPunto(obj, function (err, result) {
      success(result);
    });
  });
}
/**
 * METODO GENERICO para notificar a los trabajadores
 * HORECA sobre cualquier tipo de notificación
 */
exports.notificarTrabGenerico = async function (req, res) {
  notificaTrabGenerico(req.body);
};
async function notificaTrabGenerico(data_base_mensaje) {
  const trabajadores = await Trabajador.find({
    puntos_entrega: data_base_mensaje.punto_entrega,
  });
  for (let i in trabajadores) {
    await Notificaciones.registroNotificacion({
      tipo: data_base_mensaje.tipo,
      mensaje: data_base_mensaje.mensaje,
      destinatario: trabajadores[i]._id,
    });
  }
}

/** Crea los puntos ganados en un pedido
 *  @obj objeto de puntos ganados para pedido
 */
async function createPuntosGanados(obj) {
  return new Promise((success) => {
    Puntos_ganados_establecimiento.create(obj.data, function (err, result) {
      success(result);
    });
  });
}

/** Coloca estado redimido en codigos generados
 *  @obj id del codigo generado y nuevo estado
 */
async function setCodigoGeneradoRedimido(obj) {
  return new Promise((success) => {
    Codigo_Generado.updateById(
      obj._id,
      { estado: obj.estado, idPedido: obj.idPedido },
      function (err, result) {
        success(result);
      }
    );
  });
}

/** Lista de puntos ganados disponibles
 *  @obj id del puntos de entrega
 */
async function getPuntosGanados(obj) {
  return new Promise((success) => {
    Puntos_ganados_establecimiento.getPuntosGanadosDisponible(
      { punto_entrega: obj.punto_entrega },
      function (err, result) {
        success(result);
      }
    );
  });
}

/** Aplicar redimidos en puntos ganados establecumiento
 *  @obj id y estado del codigo
 */
async function applyRedimidos(obj) {
  return new Promise((success) => {
    Puntos_ganados_establecimiento.updateById(
      obj._id,
      { estado: obj.estado },
      function (err, result) {
        success(result);
      }
    );
  });
}

exports.get = function (req, res) {
  Pedido.get({ _id: req.params.id }, function (err, result) {
    if (!err) {
      return res.json(result);
    } else {
      return res.send(err); // 500 error
    }
  });
};

/** Información para construir la prefactura */
exports.getPrefactura = async function (req, res) {
  await Pedido.getPrefactura(
    { _id: req.params.idPedido },
    async function (err, result) {
      if (!err) {
        return res.json(result);
      } else {
        return res.send(err); // 500 error
      }
    }
  );
};

exports.getDetallado = async function (req, res) {
  const pedido = await getPedidoDetalladoPrefactura({ _id: req.params.id });
  const productosTabla = [];
  const productos = pedido[0].productos;
  for (let i in productos) {
    const productoEspecifico = productos[i].product.precios[0];
    let total = 0;
    let cantidad = "";
    if (productos[i].unidad > 0) {
      //No unidade x cant.comprada x valor unidad
      total = +total + productos[i].unidad * productoEspecifico.precio_unidad;
      cantidad = cantidad + productos[i].unidad + " unidad(es)";
    }
    if (productos[i].caja > 0) {
      //valor caja x numero de cajas
      total = +total + productos[i].caja * productoEspecifico.precio_caja;
      cantidad = cantidad + productos[i].caja + " caja(s)";
    }
    const objProducto = {
      nombre: productos[i].product.nombre,
      categoria: productos[i].product.categoria_producto.nombre,
      linea: productos[i].product.linea_producto[0].nombre,
      marca: productos[i].product.marca_producto
        ? productos[i].product.marca_producto.nombre
        : "",
      unidad_medida: productoEspecifico.unidad_medida,
      codigo_distribuidor_producto: productos[i].product
        .codigo_distribuidor_producto
        ? productos[i].product.codigo_distribuidor_producto
        : "",
      cantidad_medida: productoEspecifico.cantidad_medida,
      cantidad: cantidad,
      precio_unidad:
        "$" +
        (await InformeUtils.formatPrice(productoEspecifico.precio_unidad)),
      total: "$" + (await InformeUtils.formatPrice(total)),
    };
    productosTabla.push(objProducto);
  }

  res.status(200).json({
    success: true,
    message: "Listado detallado exitosamente",
    data: productosTabla,
  });
};

/** Crear un evento de tracking o seguimiento de pedido
 *  @obj data del evento generado para seguimeinto de pedido
 */
async function getPedidoDetalladoPrefactura(obj) {
  return new Promise((success) => {
    Pedido.getPedidoDetallado({ _id: obj._id }, function (err, result) {
      success(result);
    });
  });
}

exports.pedidosPorDist = function (req, res) {
  Pedido.getPedidosByDistribuidor(
    { distribuidor: req.params.id },
    function (err, result) {
      if (!err) {
        return res.json(result);
      } else {
        return res.send(err); // 500 error
      }
    }
  );
};

exports.cancelarPedidoPorHoreca = async function (req, res) {
  // Variables
  let data_productos = [];
  let data_pedido;
  let data_pedido_productos;

  try {
    // Get data actual pedido
    data_pedido = await Pedido.find({ _id: req.params.idPed });
    data_pedido_productos = data_pedido[0].productos;

    // Solo en estos estados se puede cancelar el pedido
    if (
      data_pedido[0].estado != "Alistamiento" &&
      data_pedido[0].estado != "Pendiente" &&
      data_pedido[0].estado != "Aprobado Interno" &&
      data_pedido[0].estado != "Aprobado Externo"
    ) {
      res.status(400).json({
        success: false,
        message:
          "Oh oh! en base al estado actual, este pedido no puede ser cancelado",
      });
      return;
    }
    if (data_pedido[0].codigo_descuento) {
      //Método para devolver los códigos de descuentos usados en el pedido
      let updateCodes = await rollbackCodesPF(
        data_pedido[0].codigo_descuento,
        req.params.idPed
      );
    }
    // Actualiza PEDIDO
    await Pedido.findOneAndUpdate(
      { _id: data_pedido[0]._id },
      { $set: { estado: "Cancelado por horeca" } },
      { new: true }
    );

    // Crea TRACKING PEDIDO
    const ultimo_tracking_ped = await Pedido_Tracking.find({
      pedido: req.params.idPed,
    }).sort({ createdAt: -1 });
    let new_tracking = {
      pedido: ultimo_tracking_ped[0].pedido,
      estado_anterior: ultimo_tracking_ped[0].estado_nuevo,
      estado_nuevo: "Cancelado por horeca",
      usuario: ultimo_tracking_ped[0].usuario,
    };
    await Pedido_Tracking.create(new_tracking);

    // Actualiza PUNTOSFT
    await Puntos_ganados_establecimiento.findOneAndUpdate(
      { pedido: data_pedido[0]._id },
      { $set: { movimiento: "Revierte" } },
      { new: true }
    );

    // Actualiza INVENTARIO PRODUCTOS
    for (const pedido_producto of data_pedido_productos) {
      let produto_inv_no_act = await Producto.find({
        _id: pedido_producto.product,
      });
      data_productos.push(
        actualizaInvProducto(produto_inv_no_act, pedido_producto)
      );
    }
    for (const producto of data_productos) {
      await Producto.findOneAndUpdate(
        { _id: producto._id },
        { $set: producto },
        { new: true }
      );
    }

    // Elimina los productos de la TABLA INTERMEDIA de reportePedidos
    await ReportePed.deleteMany({ idPedido: data_pedido[0]._id });

    // Notifica al distribuidor
    const data_notificaciones = {
      distribuidor: data_pedido[0].distribuidor,
      tipo: "Actualización de pedido",
      mensaje: `El pedido con ID: ${data_pedido[0].id_pedido} ha sido cancelado por el establecimiento, ponte en contacto con este`,
    };
    notificaTrabDistGenerico(data_notificaciones);

    // Salir exito
    res.status(201).json({
      success: true,
      message:
        "El pedido ha sido cancelado con exito y el distribuidor notificado",
      data: "result",
    });
    return;
  } catch (error) {
    res.status(400).json({
      success: false,
      message: "Error al cancelar el pedido",
      data: error,
    });
    return;
  }
};

exports.cancelarPedidoPorDistribuidor = async function (req, res) {
  // Variables
  let data_productos = [];
  let data_pedido;
  let data_pedido_productos;

  try {
    // Get data actual pedido
    data_pedido = await Pedido.find({ _id: req.params.idPed });
    data_pedido_productos = data_pedido[0].productos;

    // Solo en estos estados se puede cancelar el pedido
    if (
      data_pedido[0].estado != "Alistamiento" &&
      data_pedido[0].estado != "Pendiente" &&
      data_pedido[0].estado != "Aprobado Interno" &&
      data_pedido[0].estado != "Aprobado Externo"
    ) {
      res.status(400).json({
        success: false,
        message:
          "Oh oh! en base al estado actual, este pedido no puede ser cancelado",
      });
      return;
    }

    // Actualiza PEDIDO
    await Pedido.findOneAndUpdate(
      { _id: data_pedido[0]._id },
      { $set: { estado: "Cancelado por distribuidor" } },
      { new: true }
    );

    // Crea TRACKING PEDIDO
    const ultimo_tracking_ped = await Pedido_Tracking.find({
      pedido: req.params.idPed,
    }).sort({ createdAt: -1 });
    let new_tracking = {
      pedido: ultimo_tracking_ped[0].pedido,
      estado_anterior: ultimo_tracking_ped[0].estado_nuevo,
      estado_nuevo: "Cancelado por distribuidor",
      usuario: ultimo_tracking_ped[0].usuario,
    };
    await Pedido_Tracking.create(new_tracking);

    // Actualiza PUNTOSFT
    await Puntos_ganados_establecimiento.findOneAndUpdate(
      { pedido: data_pedido[0]._id },
      { $set: { movimiento: "Revierte" } },
      { new: true }
    );

    // Actualiza INVENTARIO PRODUCTOS
    for (const pedido_producto of data_pedido_productos) {
      let produto_inv_no_act = await Producto.find({
        _id: pedido_producto.product,
      });
      data_productos.push(
        actualizaInvProducto(produto_inv_no_act, pedido_producto)
      );
    }
    for (const producto of data_productos) {
      await Producto.findOneAndUpdate(
        { _id: producto._id },
        { $set: producto },
        { new: true }
      );
    }

    // Elimina los productos de la TABLA INTERMEDIA de reportePedidos
    await ReportePed.deleteMany({ idPedido: data_pedido[0]._id });

    // Notifica a los trabajadores del punto
    const data_notificaciones = {
      punto_entrega: data_pedido[0].punto_entrega,
      tipo: "Actualización de pedido",
      mensaje: `El pedido con ID: ${data_pedido[0].id_pedido} ha sido cancelado por el distribuidor, ponte en contacto con este`,
    };
    notificaTrabGenerico(data_notificaciones);

    // Salir exito
    res.status(201).json({
      success: true,
      message:
        "El pedido ha sido cancelado con exito y el establecimiento notificado",
      data: "result",
    });
    return;
  } catch (error) {
    res.status(400).json({
      success: false,
      message: "Error al cancelar el pedido",
      data: error,
    });
    return;
  }
};

// Función auxiliar para acualizar inventario productos
function actualizaInvProducto(producto, data_act) {
  producto[0].precios[0].inventario_unidad += data_act.unidad;
  producto[0].precios[0].inventario_caja += data_act.caja;
  return producto[0];
}

/** todos los pedidos relacionados a un ID distribuidor */
exports.AllPedidosPorDistribuidor = function (req, res) {
  Pedido.AllPedidosPorDistribuidor(
    req.params.IdDistribuidor,
    async function (err, result) {
      if (!err) {
        /** No se pueden mostrar a los distribuidores los pend. y suge. */
        result = result.filter(
          (pedido) =>
            pedido.estado !== "Sugerido"
        );
        /** Recupera la hora de aprobado y la fecha y hora de entregado */
        for (const pedido of result) {
          pedido.tracking_aprobado_externo = await getTrackingPorEstado({
            estado_nuevo: "Aprobado Externo",
            pedido: pedido._id,
          });
          if (pedido.tracking_aprobado_externo !== null) {
            const fecha_pedido_aprobado = new Date();
            fecha_pedido_aprobado.setTime(
              new Date(pedido.tracking_aprobado_externo.createdAt || '').getTime()
            );
            pedido.tracking_aprobado_externo = new Date(fecha_pedido_aprobado)
              .toLocaleString("en-US", { timeZone: "America/Bogota" })
              .split(",")[1]
              .trim();
          } else {
            pedido.tracking_aprobado_externo = "";
          }
          pedido.tracking_entregado = await getTrackingPorEstado({
            estado_nuevo: "Entregado",
            pedido: pedido._id,
          });
          if (pedido.tracking_entregado !== null) {
            const fecha_pedido_entregado = new Date();
            fecha_pedido_entregado.setTime(
              new Date(pedido.tracking_entregado.createdAt).getTime()
            );
            pedido.tracking_entregado = [
              new Date(fecha_pedido_entregado)
                .toLocaleString("en-US", { timeZone: "America/Bogota" })
                .split(",")[1]
                .trim(),
              fecha_pedido_entregado,
            ];
          } else {
            pedido.tracking_entregado = ["", ""];
          }
        }
        return res.json(result);
      } else {
        return res.send(err);
      }
    }
  );
};

/** todos los pedidos relacionados a un ID distribuidor */
exports.AllPedidosSugeridosPorDistribuidor = function (req, res) {
  Pedido.AllPedidosSugeridosPorDistribuidor(
    req.params.IdDistribuidor,
    async function (err, result) {
      if (!err) {
        /** Solo se muestran los que hayan sido en algun estado anterior sugeridos */
        result = result.filter(
          (pedido) => pedido.data_tracking_pedido_sugerido.length !== 0
        );
        /** Recupera la hora de aprobado y la fecha y hora de entregado */
        for (const pedido of result) {
          pedido.tracking_aprobado_externo = await getTrackingPorEstado({
            estado_nuevo: "Aprobado Externo",
            pedido: pedido._id,
          });
          if (pedido.tracking_aprobado_externo !== null) {
            const fecha_pedido_aprobado = new Date();
            fecha_pedido_aprobado.setTime(
              new Date(pedido.tracking_aprobado_externo.createdAt).getTime()
            );
            pedido.tracking_aprobado_externo = new Date(fecha_pedido_aprobado)
              .toLocaleString("en-US")
              .split(",")[1]
              .trim();
          } else {
            pedido.tracking_aprobado_externo = "";
          }
          pedido.tracking_entregado = await getTrackingPorEstado({
            pedido: pedido._id,
            estado_nuevo: "Entregado",
          });
          if (pedido.tracking_entregado !== null) {
            const fecha_pedido_entregado = new Date();
            fecha_pedido_entregado.setTime(
              new Date(pedido.tracking_entregado.createdAt).getTime()
            );
            pedido.tracking_entregado = [
              new Date(fecha_pedido_entregado)
                .toLocaleString("en-US")
                .split(",")[1]
                .trim(),
              fecha_pedido_entregado,
            ];
            /** Secorrige el tracking de entrega reduciendo un día */
            pedido.tracking_entregado[1] = new Date(
              pedido.tracking_entregado[1].setDate(
                pedido.tracking_entregado[1].getDate() - 1
              )
            );
          } else {
            pedido.tracking_entregado = ["", ""];
          }
        }
        return res.json(result);
      } else {
        return res.send(err); // 500 error
      }
    }
  );
};
/** calificaciones relacionadas a un ID distribuidor */
exports.calificacionPorDistribuidor = function (req, res) {
  Pedido.calificacionPorDistribuidor(
    req.params.IdDistribuidor,
    function (err, result) {
      if (!err) {
        /** Se retirar los objetos que no tengan calificaciones */
        result = result.filter((element) => {
          return element.calificacion;
        });
        /** Promedio de calificación por item */
        var avg_abastecimiento = 0;
        var avg_precio = 0;
        var avg_puntualidad_entrega = 0;
        for (const iterator of result) {
          avg_abastecimiento += parseFloat(
            iterator.calificacion.abastecimiento
          );
          avg_precio += parseFloat(iterator.calificacion.precio);
          avg_puntualidad_entrega += parseFloat(
            iterator.calificacion.puntualidad_entrega
          );
        }
        avg_abastecimiento = avg_abastecimiento / result.length || 0;
        avg_precio = avg_precio / result.length || 0;
        avg_puntualidad_entrega = avg_puntualidad_entrega / result.length || 0;
        const ponderado =
          (avg_abastecimiento + avg_precio + avg_puntualidad_entrega) / 3 || 0;
        /** Se arma objeto para respuesta */
        result = {
          distribuidor: req.params.IdDistribuidor,
          calificacion: {
            abastecimiento: avg_abastecimiento.toFixed(1),
            precio: avg_precio.toFixed(1),
            puntualidad_entrega: avg_puntualidad_entrega.toFixed(1),
            ponderado: ponderado.toFixed(1),
          },
        };
        /** Actualiza el ranking en objeto distribuidor */
        const ranking = { ranking: ponderado.toFixed(1) };
        Distribuidor.updateById(
          req.params.IdDistribuidor,
          ranking,
          function (err, result) {
            // if (!err) {
            //   res.status(200);
            // } else {
            //   res.send(err); // 500 error
            // }
          }
        );
        return res.json(result);
      } else {
        return res.send(err); // 500 error
      }
    }
  );
};

/**Traer un pedido segun el punto de entrega
 * @obj id del punto de entrega
 */
exports.getPedidoByPunto = function (req, res) {
  Pedido.getPedidosByPuntoEntrega(
    { punto_entrega: req.params.id },
    function (err, result) {
      if (!err) {
        return res.json(result);
      } else {
        return res.send(err); // 500 error
      }
    }
  );
};
/**Traer un pedido segun el punto de entrega
 * @obj id del punto de entrega
 */
exports.getPedidosByEstablecimiento = function (req, res) {
  Pedido.AllPedidosPorHoreca(
    { usuario_horeca: req.params.idHoreca },
    function (err, result) {
      if (!err) {
        return res.json(result);
      } else {
        return res.send(err); // 500 error
      }
    }
  );
};

exports.getPedidoCountByPunto = function (req, res) {
  Pedido.getPedidosByPuntoEntrega(
    { punto_entrega: req.params.id },
    function (err, result) {
      if (!err) {
        let count = result.length;
        return res.json({
          cantidad_pedidos: count,
        });
      } else {
        return res.send(err); // 500 error
      }
    }
  );
};

exports.getPedidosByPuntoDist = async function (req, res) {
  Pedido.getPedidosByPuntoDist(
    {
      punto_entrega: req.params.idPunto,
      distribuidor: req.params.idDist,
    },
    async function (err, result) {
      if (!err) {
        var orders = [];
        for (let index = 0; index < result.length; index++) {
          orders.push({
            total_pedido: result[index].total_pedido,
            id_pedido: result[index].id_pedido,
            puntos_ganados: result[index].puntos_ganados,
            fecha: result[index].fecha,
          });
        }
        if (orders.length > 0) {
          return res.json({
            distribuidor: result[0].distribuidor,
            pedidos: orders,
          });
        } else {
          return res.json(null);
        }
      } else {
        return res.send(err); // 500 error
      }
    }
  );
};

exports.getPedidosByPuntoDistCount = async function (req, res) {
  Pedido.getPedidosByPuntoDist(
    {
      punto_entrega: req.params.idPunto,
      distribuidor: req.params.idDist,
      query: {
        estado: {
          $nin: [
            "Rechazado", //admin del horeca
            "Cancelado por horeca", //admin del horeca
            "Cancelado por distribuidor",
            "Sugerido",
            "Pendiente", //creado pendiente por aprobacion de admin horeca
            "Aprobado Interno", //admin del horeca
            "Aprobado Externo", //distribuidor
            "Alistamiento",
            "Despachado",
            "Facturado",
          ],
        },
      },
    },
    async function (err, result) {
      if (!err) {
        return res.json({
          pedidos: result.length,
        });
      } else {
        return res.send(err); // 500 error
      }
    }
  );
};

exports.getAll = function (req, res) {
  Pedido.getAll({}, function (err, result) {
    if (!err) {
      return res.json(result);
    } else {
      return res.send(err); // 500 error
    }
  });
};

/** Actualizando pedido
 *  @obj id del pedido a filtrar
 *  @data nueva data a cambiar
 */
async function updatePed(id, data) {
  return new Promise((success) => {
    Pedido.updateById(id, data, function (err, result) {
      if (!err) {
        success(true);
      } else {
        success(false);
      }
    });
  });
}
/** Devolviendo códigos a estado natual
 *  @obj id del pedido a filtrar
 *  @data nueva data a cambiar
 */
async function rollbackCodesPF(data, idPedido) {
  return new Promise(async (success) => {
    for (let i in data) {
      await setCodigoGeneradoRedimido({
        _id: data[i],
        estado: "Generado",
        idPedido: idPedido,
      });
    }
    success(true);
  });
}
/** Actualiza la información y movimientos de los puntos
 *  @obj id del pedido a filtrar
 *  @data nueva data a cambiar
 */
async function updatePuntosGanados(id, data) {
  return new Promise((success) => {
    Puntos_ganados_establecimiento.updateById(
      id,
      data,
      function (err, result_puntos) {
        if (!err) {
          success(result_puntos);
        } else {
          success(err);
        }
      }
    );
  });
}
/**
 * Método para comparar la fecha de califiacion con el limite de tiempo permitido para calificar y recibir PF
 * @returns
 */
async function verificarFechaParaRedimir(fechaPedido) {
  return new Promise((success) => {
    Configuration_Model.lastCodFeat(function (err, result) {
      if (!err) {
        let meses = result.max_months_to_qualify_order;
        let fechaLimite = moment(fechaPedido).add(meses, "months");
        let fechaActual = moment();
        let diasRestantes = fechaLimite.diff(fechaActual, "days");
        if (diasRestantes === 0 || diasRestantes < 0) {
          success(true);
        } else {
          success(false);
        }
      } else {
        success(false);
      }
    });
  });
}
exports.update = async function (req, res) {
  let notificacion = [];
  let updatePuntosRegistrados;
  let movimiento_actual;
  //Validación existencia del pedido
  const pedido = await getPedido({ _id: req.params.idPedido });
  movimiento_actual = await getPedidoMovimiento(req.params.idPedido);
  if (pedido._id) {
    //Si el pedido existe se valida si el estado entrante es 'Calificado'
    if (req.body.calificacion) {
      //Se valida que existan las variables necesarias para una calificación.
      if (
        !req.body.calificacion.abastecimiento &&
        !req.body.calificacion.precio &&
        !req.body.calificacion.puntualidad_entrega
      ) {
        //Respuesta en caso de que alguno de los campos requerido no existan.
        res.send("La calificación del pedido no cumple con  los criterios");
        return false;
      }
    }
    let actualizaPedido;
    actualizaPedido = await updatePed(req.params.idPedido, req.body);
    if (actualizaPedido) {
      //Actualiza chat del pedido
      let chat = await changeMensajes({ body: req.body, pedido: pedido._id });
      //Obtengo el último estado del pedido en el traking
      let pedidoUltimoEstado = await getPedidoUltimoEstado({
        pedido: req.params.idPedido,
      });
      //Construimos el nuevo objeto para crear nuevo traking
      let data = {
        pedido: req.params.idPedido,
        estado_anterior: pedidoUltimoEstado.estado_nuevo,
        estado_nuevo: req.body.estado,
        usuario: req.params.idTrabajador,
      };
      // Actualizamos el nuevo estado en el traking
      let tracking = await createPedidoTracking({ data: data });
      //Construimos el nuevo objeto para crear nuevo traking
      let updateRep = {
        estaAntTraking: pedidoUltimoEstado.estado_nuevo,
        estaActTraking: req.body.estado,
      };
      // Actualizamos el nuevo estado en todos registros del reporte pertenecientes al pedido
      let updateReport = await updateAllReport(req.params.idPedido, updateRep);
      //Bandera para saber si actualizar o no los puntos del usuario
      let aplica_movimiento = false;
      let revierte_codigos = false;
      let devuelveInventario = false;
      let permiteRedimir = false;
      let movimiento = "";
      //Validamos el estado entrante
      if (
        req.body.estado === "Rechazado" ||
        req.body.estado === "Cancelado por horeca" ||
        req.body.estado === "Cancelado por Distribuidor"
      ) {
        revierte_codigos = true;
        aplica_movimiento = true;
        devuelveInventario = true;
        movimiento = "Revierte";
      } else if (req.body.estado === "Entregado") {
        aplica_movimiento = true;
        movimiento = "Por congelar";
      } else if (
        req.body.estado === "Calificado"
        //|| req.body.estado === "Recibido"
      ) {
        /*
        Acá debe ir configuración para verificar si el pedido calificado esta dentro del tiempo establecido
        Si está dentro del tiempo se deja congelado y se aplica movimiento, de lo contrario revierte.
        */
        aplica_movimiento = true;
        movimiento = "Congelados";
        //Función para calcular si la fecha de calificado es mayor a la fecha permitida.
        permiteRedimir = await verificarFechaParaRedimir(pedido.createdAt);
        if (permiteRedimir) {
          movimiento = "Congelados feat";
        }
      } else {
        aplica_movimiento = false;
      }
      if (aplica_movimiento) {
        if (revierte_codigos && pedido.codigo_descuento) {
          //Método para devolver los códigos de descuentos usados en el pedido
          let updateCodes = await rollbackCodesPF(
            pedido.codigo_descuento,
            req.params.idPedido
          );
        }
        if (devuelveInventario) {
          for (const pedido_producto of pedido.productos) {
            let produto_inv_no_act = await Producto.find({
              _id: pedido_producto.product,
            });
            data_productos.push(
              actualizaInvProducto(produto_inv_no_act, pedido_producto)
            );
          }
        }

        if (movimiento_actual) {
          let data = {
            punto_entrega: pedido.punto_entrega,
            pedido: req.params.idPedido,
            puntos_ganados: pedido.puntos_ganados,
            movimiento: movimiento,
            fecha: moment(),
          };
          updatePuntosRegistrados = await updatePuntosGanados(
            movimiento_actual._id,
            data
          );
        }
      }
      //
      //Validación de estados para notificaciones
      //Notificacion de pedido Aprobado
      if (req.body.estado === "Aprobado Externo") {
        //Mensaje
        let mensajeArmado =
          "¡Tu pedido ID: " +
          pedido.id_pedido +
          " ha sido aprobado por el distribuidor!";
        //Destinatarios
        let trabajadores = await getTrabajadoresDelPunto({
          punto_entrega: pedido.punto_entrega,
          usuario_horeca: pedido.usuario_horeca,
        });
        await InformeUtils.extractIds(trabajadores);
        //Envio Notificacion
        for (let i in trabajadores) {
          if (trabajadores[i].device_token !== "") {
            Notificaciones.sendNotification(
              {
                notification: {
                  title: "Feat.",
                  body: `¡Tu pedido ID: ${pedido.id_pedido} ha sido aprobado por el distribuidor!`,
                  sound: "default",
                },
              },
              trabajadores[i].device_token
            );
          }

          notificacion.push(
            await Notificaciones.registroNotificacion({
              tipo: "Actualización de pedido",
              mensaje: mensajeArmado,
              destinatario: trabajadores[i]._id,
            })
          );
        }
      }
      // Notificacion de Puntos
      //Se cambia la condicion de recibido por calificado
      if (req.body.estado === "Calificado") {
        //Mensaje
        let mensajeArmado = `¡Has recibido ${pedido.puntos_ganados} puntos Feat por tu pedido ${pedido.id_pedido}!`;
        //Destinatarios
        let trabajadores = await getTrabajadoresDelPunto({
          punto_entrega: pedido.punto_entrega,
          usuario_horeca: pedido.usuario_horeca,
        });
        let listaTrabajadores = await InformeUtils.extractIds(trabajadores);
        //Envio Notificacion
        for (let i in trabajadores) {
          if (trabajadores[i].device_token !== "") {
            Notificaciones.sendNotification(
              {
                notification: {
                  title: "Feat.",
                  body: mensajeArmado,
                  sound: "default",
                },
              },
              trabajadores[i].device_token
            );
          }
          notificacion.push(
            await Notificaciones.registroNotificacion({
              tipo: "Puntos Feat",
              mensaje: mensajeArmado,
              destinatario: trabajadores[i]._id,
            })
          );
        }
        //Registro de redención para organizaciones bolsa puntos
        //Se comenta por posibles errores en logica de funcionamiento.
        //await setRedencionPuntosFTOrganizacion({
        //_id: req.params.idPedido,
        //});
      }
      let resumen_res = {
        estado_anterior: pedidoUltimoEstado.estado_nuevo,
        estado_nuevo: req.body.estado,
        tracking: tracking,
        puntos_ganados: updatePuntosRegistrados,
        chat: chat,
        notificacion: notificacion,
        // organizacionPuntosFT: organizacionPuntosFT,
      };
      res.json(resumen_res);
    } else {
      res.send("Error al actualizar el pedido, por favor vuelve a intentarlo");
      return false;
    }
  } else {
    return res.send("No hay pedidos asociados al id ingresado");
  }
};
async function updateAllReport(id, data) {
  return new Promise((success) => {
    ReportePed.updateMany({ idPedido: id }, data, function (err, docs) {
      if (err) {
        success("Error al actualizar masivamente reportePedidos");
      } else {
        success("reportePedidos Actualizados correctamente");
      }
    });
  });
}
/** Registra y acualiza la gestion de putnos en organizaciones para lo s productos de un pedido
 *  @obj id del pedido
 */
async function setRedencionPuntosFTOrganizacion(_id) {
  const res = [];
  const pedido = await getPedidoDetalle({ _id: _id });
  //Recorrido de productos
  //registro de redencion organizacion
  for (let i in pedido[0].productos) {
    //validacion de organizacion - solo producto que esten asociados a una organizacion
    if (pedido[0].productos[i].product.codigo_organizacion) {
      //validar si tiene puntos asignados en el momento de la compra
      if (
        pedido[0].productos[i].puntos_ft_caja ||
        pedido[0].productos[i].puntos_ft_unidad
      ) {
        //validacion completa de datos de la compra
        if (
          pedido[0].productos[i].precio_compra &&
          pedido[0].productos[i].unidad_medida &&
          pedido[0].productos[i].cantidad_medida
        ) {
          //validacion bolsa de puntos organizacion (buscar,aplicar)
          const puntos =
            //pedido[0].productos[i].puntos_ft_caja ??0 +
            pedido[0].productos[i].puntos_ft_unidad ?? 0;
          const bolsa = await getBolsaOrganizacion({
            organizacion:
              pedido[0].productos[i].product.marca_producto.organizacion,
            puntosft: puntos,
          });
          //ajustar bolsa con datos nuevos
          if (bolsa[0] === undefined || bolsa[0] === null) {
            res.push({
              producto: pedido[0].productos[i].product._id,
              res: "bolsa de puntos para organizacion no encontrada",
            });
          } else {
            //registro de redencion
            const redencion = await createRedencionPuntoFT({
              puntos_ft: puntos,
              pedido: pedido[0]._id,
              distribuidor: pedido[0].distribuidor._id,
              producto: pedido[0].productos[i].product._id,
              bolsa_puntos: bolsa[0]._id,
              organizacion:
                pedido[0].productos[i].product.marca_producto.organizacion,
              precio_compra: pedido[0].productos[i].precio_compra,
              unidad_medida: pedido[0].productos[i].unidad_medida,
              cantidad_medida: pedido[0].productos[i].cantidad_medida,
            });
            //actualizacion de bolsa
            const actualizacion = await actualizaBolsaPuntoFT({
              id: bolsa[0]._id,
              data: {
                puntos_ft_redimidos: bolsa[0].puntos_ft_redimidos + puntos,
                puntos_ft_disponibles: bolsa[0].puntos_ft_disponibles - puntos,
              },
            });
            res.push({
              producto: pedido[0].productos[i].product._id,
              redencion: redencion,
              actualizacion: actualizacion,
            });
          }
        } else {
          res.push({
            producto: pedido[0].productos[i].product._id,
            puntos:
              "Datos incompletos en compra para el registro de puntos organizaciones",
          });
        }
      }
    }
  }

  return res;
}

/** Actualiza la bolsa de puntos de organizaciones con los puntos aplicados
 *  @obj id de la bolsa aplicar, data la informacion calculada de los puntos redimidos y disponibles
 */
async function actualizaBolsaPuntoFT(obj, res) {
  return new Promise((success) => {
    BolsaPuntosFTOrganizaciones.updateById(
      obj.id,
      obj.data,
      function (err, result) {
        success(result);
      }
    );
  });
}

/** Registra redencion de puntos de productos en organizaciones para informe
 *  @obj parametros de la redencion de puntos ft organizaciones
 */
async function createRedencionPuntoFT(obj, res) {
  return new Promise((success) => {
    RedencionPuntosFTOrganizaciones.create(obj, function (err, result) {
      success(result);
    });
  });
}
exports.pedido_update = async function (req, res) {
  Pedido.updateById(req.params.idPedido, req.body, function (err, result) {
    if (!err) {
      res.status(200).json({
        success: true,
        message: "Actualizada correctamente",
        data: result,
      });
    } else {
      res.send(err); // 500 error
    }
  });
};
exports.updatePedido = async function (req, res) {
  Pedido.updateById(req.params.idPedido, req.body, function (err, result) {
    if (!err) {
      res.status(200).json({
        success: true,
        message: "Actualizada correctamente",
        data: result,
      });
    } else {
      res.send(err); // 500 error
    }
  });
};
exports.editEstadoAdmin = async function (req, res) {
  Pedido.get({ _id: req.params.idPedido }, async function (err, result) {
    if (!err) {
      Pedido.updateById(req.params.idPedido, req.body, async function (err2, result2) {
        if (!err2) {
          let updateTraking = {
            pedido: req.params.idPedido,
            usuario: result2.trabajador,
            estado_anterior: result.estado,
            estado_nuevo: req.body.estado,
          }
          const updateReporte = await updateReportePedido(req.params.idPedido, { estaAntTraking: result.estado, estaActTraking: req.body.estado });
          const tracking = await createPedidoTracking({ data: updateTraking });
          if (tracking._id) {
            res.status(200).json({
              success: true,
              message: "Actualizada correctamente",
              data: result2,
            });
          }
        } else {
          res.send(err2); // 500 error
        }
      });
    } else {
      return res.send(err); // 500 error
    }
  });
};
/** Servicio para traer el listado de trabajadores del punto para envio de notificacion
 *  @obj id del punto de entrega
 */
async function getTrabajadoresDelPunto(obj) {
  return new Promise((success) => {
    Trabajador.getTrabajadoresByPuntoInfo(obj, function (err, result) {
      success(result);
    });
  });
}

/** Servicio para traer el listado de trabajadores del punto para envio de notificacion
 *  @obj id del punto de entrega
 */
async function getTrabajadoresDelDistribuidor(obj) {
  return new Promise((success) => {
    Trabajador.getTrabajadoresByDistribuidorInfo(obj, function (err, result) {
      success(result);
    });
  });
}

/** Aplicar Cambios segun el cambio de estado en el historico de mensajes de chat
 *  @obj body de la peticion con el estado
 */
async function changeMensajes(obj) {
  if (
    obj.estado === "Recibido" ||
    obj.estado === "Calificado" ||
    obj.estado === "Rechazado" ||
    obj.estado === "Cancelado por horeca" ||
    obj.estado === "Cancelado por distribuidor"
  ) {
    return new Promise((success) => {
      Mensaje.updateByPedido(
        { pedido: obj.pedido },
        { estado: "Inactivo" },
        function (err, result) {
          success(result);
        }
      );
    });
  } else {
    return "";
  }
}

/** Traer un pedido en particular
 *  @obj id del pedido a filtrar
 */
async function getPedido(obj) {
  return new Promise((success) => {
    Pedido.get(obj, function (err, result) {
      success(result);
    });
  });
}

/** Traer bolsa de puntos organizacion para aplicar resta
 *  @obj id del pedido a filtrar
 */
async function getBolsaOrganizacion(obj) {
  return new Promise((success) => {
    BolsaPuntosFTOrganizaciones.getBolsaOrganizacion(
      obj,
      function (err, result) {
        success(result);
      }
    );
  });
}

/** Traer un pedido en detallado
 *  @obj id del pedido a filtrar
 */
async function getPedidoDetalle(obj) {
  return new Promise((success) => {
    Pedido.getPedidoDetallado(obj, function (err, result) {
      success(result);
    });
  });
}

/** Traer un pedido en particular
 *  @obj id del pedido a filtrar
 */
async function getPedidoUltimoEstado(obj) {
  return new Promise((success) => {
    Pedido_Tracking.lastStatusPedido(
      { pedido: obj.pedido },
      function (err, result) {
        success(result);
      }
    );
  });
}

/** Crear un evento de tracking o seguimiento de pedido
 *  @obj data del evento generado para seguimeinto de pedido
 */
async function createPedidoTracking(obj) {
  return new Promise((success) => {
    Pedido_Tracking.create(obj.data, function (err, result) {
      success(result);
    });
  });
}
/** Actualizar reporte del pedido migrado del estado
 *  @obj data del evento generado para actualiza pedido
 */
async function updateReportePedido(idPed, obj) {
  return new Promise((success) => {
    ReportePed.updateMany(
      { idPedido: idPed },
      obj,
      function (err, docs) {
        if (err) {
          let resultado = {
            success: false,
            err: err
          }
          success(resultado);
        } else {
          let resultado = {
            success: true,
            docs: docs
          }
          success(resultado);
        }
      }
    );
    /*Pedido_Tracking.create(obj.data, function (err, result) {
      success(result);
    });*/
  });
}
/** Crear un evento de tracking o seguimiento de pedido
 *  @obj data del evento generado para seguimeinto de pedido
 */
async function createHistoricoPedidosDistribuidor(data, pedido, traking) {
  return new Promise(async (success) => {
    let i = 0;
    let dataInsert = [];
    const distribuidorId = pedido.distribuidor;
    for (let prod of data.listProductos) {
      let puntosFeatUnidad = 0;
      if(prod.mostrarPF){
        puntosFeatUnidad = prod.precios
        ? prod.precios[0].puntos_ft_unidad
        : "";
      }
      let puntosFeatCaja = prod.precios ? prod.precios[0].puntos_ft_caja : "";
      let puntos_redimidos = prod.puntos_redimidos ? prod.puntos_redimidos : "";
      let id_org_producto = 0;
      let costoProductos = data.listProductos[i].precios[0].precio_unidad;
      if (
        data.listProductos[i]?.prodDescuento &&
        data.listProductos[0]?.prodPorcentajeDesc
      ) {
        costoProductos = Math.round(
          costoProductos -
          costoProductos * (data.listProductos[i]?.prodPorcentajeDesc / 100)
        );
      }

      let constructorData = {
        //Id o llaves principales para busquedas
        idPedido: traking.pedido,
        idOrganizacion: data.listProductos[i].codigo_organizacion,
        // idDistribuidor: data.listProductos[i].codigo_distribuidor,
        idDistribuidor: distribuidorId,
        idComprador: traking.usuario,
        idUserHoreca: pedido.usuario_horeca,
        idTraking: traking._id,
        idPunto: pedido.punto_entrega,
        productoId: data.listProductos[i]._id,
        tipoUsuario: data.tipoNegocioUser,
        //Data pedido
        ciudad: pedido.ciudad,
        estaAntTraking: traking.estado_anterior,
        estaActTraking: traking.estado_nuevo,
        //Data producto
        nombreProducto: data.listProductos[i].nombre,
        codigoFeatProducto: data.listProductos[i].codigo_ft,
        codigo_organizacion_producto:
          data.listProductos[i].codigo_organizacion_producto,
        codigoDistribuidorProducto:
          data.listProductos[i].codigo_distribuidor_producto,
        categoriaProducto: data.listProductos[i].categoria_producto,
        marcaProducto: data.listProductos[i].marca_producto,
        lineaProducto: data.listProductos[i].linea_producto[0],
        caja: prod.caja,
        unidadesCompradas: parseFloat(data.productos[i].unidad),
        precioEspecial: data.productos[i].precioEspecial,
        costoProductos,
        referencia: "",
        puntos_ft_unidad: parseFloat(puntosFeatUnidad),
        puntos_ft_caja: parseFloat(puntosFeatCaja),
        puntos_ft_ganados:
          parseFloat(puntosFeatUnidad) * parseFloat(data.productos[i].unidad),
        puntoCiudad: data.puntoEntrega.ciudad,
        puntoDomicilio: data.puntoEntrega.domicilios,
        puntoSillas: data.puntoEntrega.sillas,
        totalCompra: data.total_pedido,
        subtotalCompra: data.subtotal_pedido,
        descuento: data.descuento_pedido,
        puntosGanados: data.puntos_ganados,
        puntosRedimidos: puntos_redimidos,
        detalleProducto: data.listProductos[i],
      };
      if (!data.listProductos[i].codigo_organizacion) {
        delete constructorData.idOrganizacion;
      }
      i++;
      dataInsert.push(constructorData);
    }
    if (data.puntos_ganados > 0) {
      let org = await obteneOrgBolsas();
      if (org.length > 0) {
        //let resOrgRecorridas = await verificarOrganizaciones(org);
      }
    }
    ReportePed.insertMany(dataInsert, function (err, docs) {
      if (err) {
        success("Error al insertar lista ");
      } else {
        success("Insertados correctamente");
      }
    });
  });
}
/** Crear un evento de tracking o seguimiento de pedido
 *  @obj data del evento generado para seguimeinto de pedido
 */
async function createHistoricoPedidosDistribuidorObject(productos, data, traking, idPedido, puntoEntrega, establecimiento) {
  return new Promise(async (success) => {
    let i = 0;
    let dataInsert = [];
    for (let prod of productos) {
      const pedidoProducto = data.productos.find(p => p.product.toString() === prod._id.toString());

      let puntosFeatUnidad = pedidoProducto.puntos_ft_unidad;
      let puntosFeatCaja = pedidoProducto.puntos_ft_caja;
      let puntos_redimidos = prod.puntos_redimidos ? prod.puntos_redimidos : "";
      let precio_caja_app = pedidoProducto.precio_caja
      let costoProductos = pedidoProducto.precio_original;


      let constructorData = {
        //Id o llaves principales para busquedas
        idPedido: idPedido,
        idOrganizacion: prod.codigo_organizacion,
        // idDistribuidor: data.listProductos[i].codigo_distribuidor,
        idDistribuidor: data.distribuidor,
        idComprador: data.trabajador,
        idUserHoreca: data.usuario_horeca,
        idTraking: traking,
        idPunto: data.punto_entrega,
        productoId: prod._id,
        //Data pedido
        ciudad: data.ciudad,
        estaAntTraking: data.estado,
        estaActTraking: data.estado,
        //Data producto
        nombreProducto: prod.nombre,
        precio_caja_app: precio_caja_app,
        codigoFeatProducto: prod.codigo_ft,
        codigo_organizacion_producto: prod.codigo_organizacion_producto,
        codigoDistribuidorProducto: prod.codigo_distribuidor_producto,
        categoriaProducto: prod.categoria_producto,
        marcaProducto: prod.marca_producto,
        lineaProducto: prod.linea_producto[0],
        caja: prod.caja,
        unidadesCompradas: parseFloat(pedidoProducto.unidadesCompradas),
        precioEspecial: prod.precioEspecial || {},
        costoProductos: costoProductos,
        referencia: "",
        puntos_ft_unidad: parseFloat(puntosFeatUnidad),
        puntos_ft_caja: parseFloat(puntosFeatCaja),
        puntos_ft_ganados: parseFloat(puntosFeatUnidad) * parseFloat(pedidoProducto.unidadesCompradas),
        totalCompra: data.total_pedido,
        subtotalCompra: data.subtotal_pedido,
        descuento: data.descuento_pedido,
        puntosGanados: parseFloat(puntosFeatUnidad) * parseFloat(pedidoProducto.unidadesCompradas),
        puntosRedimidos: puntos_redimidos,
        detalleProducto: prod,
        //data tipo de negocio
        tipoUsuario: establecimiento.tipo_negocio,
        //data Punto Entrega
        puntoCiudad: puntoEntrega.ciudad,
        puntoDomicilio: puntoEntrega.domicilios,
        puntoSillas: puntoEntrega.sillas,
      };
      if (!prod.codigo_organizacion) {
        delete constructorData.idOrganizacion;
      }
      i++;
      dataInsert.push(constructorData);
    }
    if (data.puntos_ganados > 0) {
      let org = await obteneOrgBolsas();
      if (org.length > 0) {
        let resOrgRecorridas = await verificarOrganizaciones(org);
      }
    }
    success(dataInsert);

  });
}
/**
 * Metodos para buscar y verificar las bolsas
 */
function obteneOrgBolsas() {
  return new Promise((succes, reject) => {
    BolsaPuntosFTOrganizaciones.getPuntosORGPedidosAll(
      {},
      function (err, result) {
        if (!err) {
          let indicadores = {
            puntos_totales: 0,
            puntos_usados: 0,
            puntos_disponibles: 0,
          };
          let newResult = [];
          for (let punto of result) {
            let estados_Sugerido = 0;
            let estados_Pendiente = 0;
            let estados_aprobado_interno = 0;
            let estados_aprobado_externo = 0;
            let estados_Alistamiento = 0;
            let estados_Despachado = 0;
            let estados_Facturado = 0;
            let estados_Entregado = 0;
            let estados_Recibido = 0;
            let estados_Calificado = 0;
            let estados_Rechazado = 0;
            for (let agrupados of punto.data_pedidos) {
              switch (agrupados._id) {
                case "Aprobado Interno":
                  estados_aprobado_interno = agrupados.totalAcumulado;
                  break;
                case "Aprobado Externo":
                  estados_aprobado_externo = agrupados.totalAcumulado;
                  break;
                case "Sugerido":
                  estados_Sugerido = agrupados.totalAcumulado;
                  break;
                case "Pendiente":
                  estados_Pendiente = agrupados.totalAcumulado;
                  break;
                case "Alistamiento":
                  estados_Alistamiento = agrupados.totalAcumulado;
                  break;
                case "Despachado":
                  estados_Despachado = agrupados.totalAcumulado;
                  break;
                case "Facturado":
                  estados_Facturado = agrupados.totalAcumulado;
                  break;
                case "Entregado":
                  estados_Entregado = agrupados.totalAcumulado;
                  break;
                case "Recibido":
                  estados_Recibido = agrupados.totalAcumulado;
                  break;
                case "Calificado":
                  estados_Calificado = agrupados.totalAcumulado;
                  break;
                case "Rechazado":
                  estados_Rechazado = agrupados.totalAcumulado;
                  break;
              }
              let operacion = "estados_" + agrupados._id;
              operacion = agrupados.totalAcumulado;
            }
            if (punto.comparativa95_porciento == "Si") {
              punto.comparativa60_porciento = "No";
            }
            newResult.push({
              idOrganizacion: punto.data_organizacion[0]._id,
              nombreOrganizacion: punto.data_organizacion[0].nombre,
              numero_paquetes: punto.cant_paquetes_comprados,
              total_bolsa: punto.total_puntos_ft_bolsa,
              porcentaje_95_total_bolsa: punto.monto_95_porciento_total,
              total_consumido_pedidos: punto.total_puntos_ft_descontados,
              porcentaje_consumido:
                (punto.total_puntos_ft_descontados * 100) /
                punto.total_puntos_ft_bolsa,
              pedidos_Sugerido: estados_Sugerido,
              pedidos_Pendiente: estados_Pendiente,
              pedidos_aprobado_interno: estados_aprobado_interno,
              pedidos_aprobado_externo: estados_aprobado_externo,
              pedidos_Alistamiento: estados_Alistamiento,
              pedidos_Despachado: estados_Despachado,
              pedidos_Facturado: estados_Facturado,
              pedidos_Entregado: estados_Entregado,
              pedidos_Recibido: estados_Recibido,
              pedidos_Calificado: estados_Calificado,
              pedidos_Rechazado: estados_Rechazado,
              bolsa_a_tope95_porciento: punto.bolsa_a_tope95_porciento,
              bolsa_a_tope60_porciento: punto.bolsa_a_tope60_porciento,
            });
            indicadores.puntos_totales =
              indicadores.puntos_totales + punto.total_puntos_ft_bolsa;
            indicadores.puntos_usados =
              indicadores.puntos_usados + punto.total_puntos_ft_descontados;
          }
          //validarBolsas(newResult);
          return succes(newResult);
        } else {
          return succes(err);
        }
      }
    );
  });
}
function verificarOrganizaciones(bolsas) {
  return new Promise((succes, reject) => {
    bolsas.forEach(async (bolsa) => {
     /* if (bolsa.porcentaje_consumido >= 90) {
        let actualiza = {
          mostrarPF: false,
          comentarioVencimiento:
            "Bolsa de organizacion ha consumido el 90% o superior",
        };
        Producto.updateMany(
          { codigo_organizacion: bolsa.idOrganizacion },
          actualiza,
          function (err, docs) {
            if (err) {} else {}
          }
        );
      }*/
    });
    succes("bolsas recorridas");
  });
}

/** Traer movimientos que ha tenido un pedido
 *  @obj objeto de pedido
 */
async function getPedidoMovimiento(obj) {
  return new Promise((success) => {
    Puntos_ganados_establecimiento.get({ pedido: obj }, function (err, result) {
      if (!err) {
        success(result);
      } else {
        success(err);
      }
    });
  });
}

exports.delete = function (req, res) {
  Pedido.removeById({ _id: req.params.id }, function (err, result) {
    if (!err) {
      return res.json(result);
    } else {
      return res.send(err); // 500 error
    }
  });
};

exports.ultimoEstadoPedido = function (req, res) {
  Pedido_Tracking.lastStatusPedido(
    { pedido: req.params.idPedido },
    function (err, result) {
      if (!err) {
        return res.json(result);
      } else {
        return res.send(err); // 500 error
      }
    }
  );
};

/** Recupera la fecha de aprobación de un pedido por el distribuidor */
exports.trackingFechaAprobadoExterno = function (req, res) {
  Pedido_Tracking.trackingFechaAprobadoExterno(
    { pedido: req.params.idPedido, estado_nuevo: "Aprobado Externo" },
    function (err, result) {
      if (!err) {
        return res.json(result);
      } else {
        return res.send(err); // 500 error
      }
    }
  );
};
/** Recupera la fecha de enrega de un pedido por el distribuidor */
exports.trackingFechaEntregado = function (req, res) {
  Pedido_Tracking.trackingFechaEntregado(
    { pedido: req.params.idPedido, estado_nuevo: "Entregado" },
    function (err, result) {
      if (!err) {
        return res.json(result);
      } else {
        return res.send(err); // 500 error
      }
    }
  );
};

exports.getProductosVolver = async function (req, res) {
  const header = req.header("Authorization") || "";
  const token = header.split(" ")[1];
  if (!token) {
    return res.status(401).json({ message: "Token no proporcionado" });
  }

  let payload;
  try {
    payload = jwt.verify(token, config.secret);
  } catch (error) {
    return res.status(401).json({ message: "Token inválido" });
  }
  const session = await mongoose.startSession();
  let variaricionProductos = [];
  try {
    let pedidoCompleto
    let dataProductosFinales = [];
    let productoLimpio = [];
    await session.withTransaction(async () => {
      pedidoCompleto = await getPedidoProductosDetalladosCompletoPrecios({
        _id: req.params.id,
      });
      if (!pedidoCompleto) {
        throw new Error('Fallo al consultar pedido');
      }


      if (pedidoCompleto[0].historicoPedido) {
        for (let producto of pedidoCompleto[0].historicoPedido) {

          productoLimpio.push(producto.productoActualizado[0]);
          let precioActual = producto.productoActualizado[0].precios[0].precio_unidad
          const productoRDescuento = await descuentosPunto.findOne({
            idPunto: producto.idPunto.toString(),
            'productosDescuento.productId': producto.productoId.toString()
          }).session(session);
          if (productoRDescuento) {

            for (let prodDes of productoRDescuento.productosDescuento) {
              if (prodDes.productoID.toString() === producto.productoId.toString()) {
                if (prodDes.porcentaIncremento && prodDes.porcentaIncremento > 0) {
                  let incremento = (precioActual * prodDes.porcentaIncremento) / 100;
                  precioActual = precioActual + incremento;
                }
                if (prodDes.porcentaDescuento && prodDes.porcentaDescuento > 0) {
                  let descuento = (precioActual * prodDes.porcentaIncremento) / 100;
                  precioActual = precioActual - descuento;

                }
                if (prodDes.montoFijo && prodDes.montoFijo > 0) {
                  precioActual = prodDes.montoFijo;
                }
                let actualiza = {
                  idPedido: req.params.id,
                  idProducto: producto.productoActualizado[0]._id,
                  producto: producto.productoActualizado[0].nombre,
                  imagenProd: producto.productoActualizado[0].fotos[0],
                  precioActual: precioActual,
                  unidades_disponibles:
                    producto.productoActualizado[0].precios[0].inventario_unidad,
                  precioAnterior: producto.costoProductos,
                  unidadesPedido: producto.unidadesCompradas,
                  unidadesDisponibles:
                    producto.productoActualizado[0].precios[0].inventario_unidad,
                  comentario: "Variación",
                  precioVariado: true,
                  muestraPF: producto.productoActualizado[0].mostrarPF,
                  puntosFT: producto.productoActualizado[0].mostrarPF ? producto.productoActualizado[0].precios[0].puntos_ft_unidad : 0
                };
                variaricionProductos.push(actualiza);
              }
            }
          } else {
            if (producto.productoActualizado[0].prodPorcentajeDesc && producto.productoActualizado[0].prodPorcentajeDesc > 0) {
              let incremento = (precioActual * producto.productoActualizado[0].prodPorcentajeDesc) / 100;
              precioActual = precioActual - incremento;
            }

            if (precioActual !== producto.productoActualizado[0].precios[0].precio_unidad) {
              let actualiza = {
                idPedido: req.params.id,
                idProducto: producto.productoActualizado[0]._id,
                producto: producto.productoActualizado[0].nombre,
                imagenProd: producto.productoActualizado[0].fotos[0],
                precioActual: precioActual,
                unidades_disponibles:
                  producto.productoActualizado[0].precios[0].inventario_unidad,
                precioAnterior: producto.costoProductos,
                unidadesPedido: producto.unidadesCompradas,
                unidadesDisponibles:
                  producto.productoActualizado[0].precios[0].inventario_unidad,
                comentario: "Variación",
                precioVariado: true,
                muestraPF: producto.productoActualizado[0].mostrarPF,
                puntosFT: producto.productoActualizado[0].mostrarPF ? producto.productoActualizado[0].precios[0].puntos_ft_unidad : 0
              };
              variaricionProductos.push(actualiza);
            }
          }
        }


      }
      pedidoCompleto[0].productos.forEach(async (producto) => {
        const index = variaricionProductos.findIndex(
          (x) =>
            x.idProducto.toString().replace(/ObjectId\("(.*)"\)/, "$1") ===
            producto.product.toString().replace(/ObjectId\("(.*)"\)/, "$1")
        );
        if (index >= 0) {
          let prodData = {
            product: variaricionProductos[index].idProducto,
            dataP: "",
            unidad: variaricionProductos[index].unidadesPedido,
            unidad_disponible: variaricionProductos[index].unidadesDisponibles,
            caja: producto.caja,
            puntos_ft_unidad: variaricionProductos[index].puntosFT,
            puntos_ft_caja: producto.puntos_ft_caja,
            mostrar_pf: variaricionProductos[index].muestraPF,

          };
          dataProductosFinales.push(prodData);
        } else {
          let prodData = {
            product: producto.product,
            dataP: "",
            unidad: producto.unidad,
            unidad_disponible: producto.unidad,
            caja: producto.caja,
            puntos_ft_unidad: producto.puntos_ft_unidad,
            puntos_ft_caja: producto.puntos_ft_caja,
            mostrar_pf: false,

          };
          dataProductosFinales.push(prodData);
        }
      });
    });

    for (let pedido of dataProductosFinales) {

      for (let limpio of productoLimpio) {
        if (
          pedido.product.toString() ===
          limpio._id.toString()
        ) {
          pedido.dataCompletaP = limpio;
        }
      }
    }
    res.status(200).json({
      success: true,
      data: variaricionProductos,
      listProductos: dataProductosFinales,
      pedido_original: pedidoCompleto,
    });
  } catch (error) {
    console.error('Error during transaction:', error);
    res.status(500).send({ success: false, message: 'Ha fallado la actualización del pedido', error: error.message });
  } finally {
    await session.endSession();
  }
};

exports.getPedidoProductos = async function (req, res) {
  let valExistencia = true;
  let valInventario = true;
  let precio = true;
  let pedidoOriginal;
  let msg = "";
  let variaricionProductos = [];
  let productoLimpio = [];
  //Listado de productos
  let productosPedidoExistente = await getPedidoProductosDetallados({
    _id: req.params.id,
  });
  let pedidoCompleto = await getPedidoProductosDetalladosCompletoPrecios({
    _id: req.params.id,
  });
  let posProd = 0;
  if (pedidoCompleto[0].historicoPedido) {
    pedidoCompleto[0].historicoPedido.forEach(async (producto) => {
      productoLimpio.push(producto.productoActualizado[0]);
      let precioActual =
        producto.productoActualizado[0].precios[0].precio_unidad;
      if (
        producto.productoActualizado[0]?.prodDescuento &&
        producto.productoActualizado[0]?.prodPorcentajeDesc > 0
      ) {
        precioActual = Math.round(
          precioActual -
          precioActual *
          (producto.productoActualizado[0].prodPorcentajeDesc / 100)
        );
      }
      if (producto.costoProductos !== precioActual) {
        let actualiza = {
          idPedido: req.params.id,
          idProducto: producto.productoActualizado[0]._id,
          producto: producto.productoActualizado[0].nombre,
          imagenProd: producto.productoActualizado[0].fotos[0],
          precioActual: precioActual,
          unidades_disponibles:
            producto.productoActualizado[0].precios[0].inventario_unidad,
          precioAnterior: producto.costoProductos,
          unidadesPedido: producto.unidadesCompradas,
          unidadesDisponibles:
            producto.productoActualizado[0].precios[0].inventario_unidad,
          comentario: "Variación precio",
          precioVariado: true,
        };
        variaricionProductos.push(actualiza);
      } else {
      }
      if (
        producto.productoActualizado[0].precios[0].inventario_unidad <
        producto.unidadesCompradas
      ) {
        if (variaricionProductos.length === 0) {
          let actualiza = {
            idPedido: req.params.id,
            idProducto: producto.productoActualizado[0]._id,
            producto: producto.productoActualizado[0].nombre,
            imagenProd: producto.productoActualizado[0].fotos[0],
            precioActual:
              producto.productoActualizado[0].precios[0].precio_unidad,
            unidades_disponibles:
              producto.productoActualizado[0].precios[0].inventario_unidad,
            precioAnterior: producto.costoProductos,
            unidadesPedido: producto.unidadesCompradas,
            unidadesDisponibles:
              producto.productoActualizado[0].precios[0].inventario_unidad,
            comentario: "Variación en existencia",
            precioVariado: false,
          };
          variaricionProductos.push(actualiza);
        } else {
          if (
            variaricionProductos[variaricionProductos.length - 1].idProducto ===
            producto.productoActualizado[0]._id
          ) {
            variaricionProductos[variaricionProductos.length - 1].comentario +=
              ", Variación en existencia";
          } else {
            let actualiza = {
              idPedido: req.params.id,
              idProducto: producto.productoActualizado[0]._id,
              producto: producto.productoActualizado[0].nombre,
              imagenProd: producto.productoActualizado[0].fotos[0],
              precioActual:
                producto.productoActualizado[0].precios[0].precio_unidad,
              unidades_disponibles:
                producto.productoActualizado[0].precios[0].inventario_unidad,
              precioAnterior: producto.costoProductos,
              unidadesPedido: producto.unidadesCompradas,
              unidadesDisponibles:
                producto.productoActualizado[0].precios[0].inventario_unidad,
              comentario: "Variación en existencia",
              precioVariado: false,
            };
            variaricionProductos.push(actualiza);
          }
        }
      }
      posProd = posProd + 1;
    });
    // delete pedidoCompleto[0].historicoPedido;
  } else {
    res.status(200).json({
      data: {},
      listProductos: {},
      pedido_original: {},
    });
  }
  let dataProductosFinales = [];
  pedidoCompleto[0].productos.forEach(async (producto) => {
    const index = variaricionProductos.findIndex(
      (x) =>
        x.idProducto.toString().replace(/ObjectId\("(.*)"\)/, "$1") ===
        producto.product.toString().replace(/ObjectId\("(.*)"\)/, "$1")
    );
    if (index >= 0) {
      let prodData = {
        product: variaricionProductos[index].idProducto,
        dataP: "",
        unidad: variaricionProductos[index].unidadesPedido,
        unidad_disponible: variaricionProductos[index].unidadesDisponibles,
        caja: producto.caja,
        puntos_ft_unidad: producto.puntos_ft_unidad,
        puntos_ft_caja: producto.puntos_ft_caja,
      };
      let busca = variaricionProductos[index].comentario.includes("existencia");
      if (!busca) {
        dataProductosFinales.push(prodData);
      } else {
        if (variaricionProductos[index].unidades_disponibles > 0) {
          dataProductosFinales.push(prodData);
        }
      }
    } else {
      let prodData = {
        product: producto.product,
        dataP: "",
        unidad: producto.unidad,
        unidad_disponible: producto.unidad,
        caja: producto.caja,
        puntos_ft_unidad: producto.puntos_ft_unidad,
        puntos_ft_caja: producto.puntos_ft_caja,
      };
      dataProductosFinales.push(prodData);
    }
  });
  for (let pedido of dataProductosFinales) {
    for (let limpio of productoLimpio) {
      if (
        pedido.product.toString() ===
        limpio._id.toString()
      ) {
        pedido.dataCompletaP = limpio;
      }
    }
  }

  if (variaricionProductos.length === 0) {
    res.status(200).json({
      success: true,
      data: variaricionProductos,
      listProductos: dataProductosFinales,
      pedido_original: pedidoCompleto,
    });
  } else {
    res.status(200).json({
      success: false,
      data: variaricionProductos,
      listProductos: dataProductosFinales,
      pedido_original: pedidoCompleto,
    });
  }
};

/**
 * Traer el producto con información detallada
 * @obj id del producto
 */
async function getPedidoProductosDetallados(obj) {
  return new Promise((success) => {
    Pedido.getPedidoProductosDetallados(obj, function (err, result) {
      success(result);
    });
  });
}
/**
 * Traer el pedido con información detallada con precios del pedido y precios actuales
 * @obj id del producto
 */
async function getPedidoProductosDetalladosCompletoPrecios(obj) {
  return new Promise((success) => {
    Pedido.getPedidoProductosDetalladosVolverPedir(obj, function (err, result) {
      success(result);
    });
  });
}
/** Traer un producto por id
 *  @obj id del pedido a filtrar
 */
async function getProductoDetallado(obj) {
  return new Promise((success) => {
    Producto.getProductoValidacion(obj, function (err, result) {
      success(result);
    });
  });
}

/** Traer un producto por id
 *  @obj id del pedido a filtrar
 */
async function validateProductoEnDistribuidor(obj) {
  return new Promise((success) => {
    ProductosPorDistribuidor.getProducto(obj, function (err, result) {
      success(result.length);
    });
  });
}

exports.getInfoPedido = function (req, res) {
  Pedido.getPedidoProductosDetallados(
    { _id: req.params.id },
    function (err, result) {
      if (!err) {
        res.status(200).json({
          success: true,
          message: "Información de pedido",
          data: result,
        });
      } else {
        res.status(500).json({
          success: false,
          message: "Error en información de pedido",
          data: err,
        });
      }
    }
  );
};

exports.getProdsPorPedido = function (req, res) {
  Pedido.getPedidoProductosDetallados(
    { _id: req.params.id },
    function (err, result) {
      if (!err) {
        res.status(200).json({
          success: true,
          message: "Información de pedido",
          data: {
            productos: result[0].productos,
          },
        });
      } else {
        res.status(500).json({
          success: false,
          message: "Error en información de pedido",
          data: err,
        });
      }
    }
  );
};

exports.getPedidosPorPuntoDistFecha = function (req, res) {
  Pedido.getPedidosPorPuntoDistFecha(req.body, function (err, result) {
    if (!err) {
      let ans = result;

      //Filtrar por fechas
      if (req.body.fecha_inicial && req.body.fecha_final) {
        ans = [];

        const split_inicial = req.body.fecha_inicial.split("T")[0].split("-");
        const year_inicial = Number.parseInt(split_inicial[0]);
        const month_inicial = Number.parseInt(split_inicial[1]);
        const day_inicial = Number.parseInt(split_inicial[2]);

        const split_final = req.body.fecha_final.split("T")[0].split("-");
        const year_final = Number.parseInt(split_final[0]);
        const month_final = Number.parseInt(split_final[1]);
        const day_final = Number.parseInt(split_final[2]);

        let year = 0;
        let month = 0;
        let day = 0;
        for (const ped_aux of result) {
          year = ped_aux.fecha.getFullYear();
          month = ped_aux.fecha.getMonth() + 1;
          day = ped_aux.fecha.getDate();

          if (
            compareDates(
              [year_inicial, month_inicial, day_inicial],
              [year, month, day]
            ) >= 0 &&
            compareDates(
              [year_final, month_final, day_final],
              [year, month, day]
            ) <= 0
          ) {
            ans.push(ped_aux);
          }
        }
      }

      res.status(200).json({
        success: true,
        message: "Pedidos por punto de entrega, distribuidores, y fechas",
        data: ans,
      });
    } else {
      res.status(500).json({
        success: false,
        message: JSON.stringify(err),
        data: [],
      });
    }
  });
};

const compareDates = function (base_date, date_to_compare) {
  if (date_to_compare[0] === base_date[0]) {
    if (date_to_compare[1] === base_date[1]) {
      if (date_to_compare[2] === base_date[2]) {
        return 0;
      } else if (date_to_compare[2] > base_date[2]) {
        return 1;
      } else {
        return -1;
      }
    } else if (date_to_compare[1] > base_date[1]) {
      return 1;
    } else {
      return -1;
    }
  } else if (date_to_compare[0] > base_date[0]) {
    return 1;
  } else {
    return -1;
  }
};

exports.getChatsActivosByPunto = function (req, res) {
  Pedido.getChatsActivosByPunto(req.params.idPunto, function (err, result) {
    if (!err) {
      return res.json(result);
    } else {
      return res.send(err); // 500 error
    }
  });
};

exports.getProdutoPorPedidoPorPunto = function (req, res) {
  let categoria_producto;
  Producto.findById(req.params.id, function (err, result) {
    if (!err) {
      categoria_producto = result.categoria_producto;
      Pedido.find({}, function (err, result) {
        if (!err) {
          let dataPedido;
          let puntos = [];
          let pedidoConProducto = [];
          let pedidoConCategoria = [];
          Promise.all(
            (dataPedido = result.map((pedido) => {
              Promise.all(
                pedido.productos.map((element) => {
                  if (element.product != null) {
                    let catId = categoria_producto;
                    let catPedidosId = element.product.categoria_producto;
                    if (catPedidosId !== undefined) {
                      if (catPedidosId.toString() === catId.toString()) {
                        pedidoConCategoria.push(catPedidosId);
                      }
                    }

                    if (element.product._id === req.params.id) {
                      pedidoConProducto.push(pedido);
                      puntos.push(pedido.punto_entrega);
                    }
                  }
                })
              );
            }))
          );
          let porcentaje =
            (pedidoConProducto.length * 100) / pedidoConCategoria.length;

          return res.json({
            pedidoConProducto,
            puntos_entrega: puntos,
            pedidoConCategoria: {
              porcentaje: porcentaje.toFixed(2),
              pedidosConProducto: pedidoConProducto.length,
              pedidosConCategoria: pedidoConCategoria.length,
            },
          });
        } else {
          return res.send(err); // 500 error
        }
      }).populate({
        path: "productos.product",
        select: "categoria_producto",
      });
    } else {
      return res.send(err); // 500 error
    }
  });
};
exports.getChatsActivosByDistribuidor = function (req, res) {
  Pedido.getChatsActivosByDistribuidor(
    req.params.idDistribuidor,
    function (err, result) {
      if (!err) {
        return res.json(result);
      } else {
        return res.send(err); // 500 error
      }
    }
  );
};
exports.getTotalCalificacionesDistribuidor = function (req, res) {
  Pedido.getTotalCalificacionesDistribuidor(
    req.params.idDistribuidor,
    function (err, result) {
      if (!err) {
        const resultado = {
          cant_calificaciones: result.length,
          total_calificaciones: 0,
        };
        for (const iterator of result) {
          const promedio_calificacion =
            (Number(iterator.precio) +
              Number(iterator.puntualidad_entrega) +
              Number(iterator.abastecimiento)) /
            3;
          resultado.total_calificaciones =
            resultado.total_calificaciones + promedio_calificacion;
        }
        return res.json(resultado);
      } else {
        return res.send(err); // 500 error
      }
    }
  );
};

/** Pedido de un producto en los ultimos 3meses */
exports.getTotalPedidosProductosTresMeses = function (req, res) {
  Pedido.getTotalPedidosProductosTresMeses(
    req.params.idProducto,
    function (err, result) {
      if (!err) {
        return res.json(result);
      } else {
        return res.send(err); // 500 error
      }
    }
  );
};

/** Establecimeintos que pidieron el producto en los ultimos 3meses */
exports.countPedidoProductoEstablecimiento = function (req, res) {
  Pedido.countPedidoProductoEstablecimiento(
    req.params.idProducto,
    function (err, result) {
      if (!err) {
        return res.json(result);
      } else {
        return res.send(err); // 500 error
      }
    }
  );
};

// Recupera el total de tiempo de entrega de un pedido luego de finalizado y su detalle
exports.getTiempoEntregaPedido = function (req, res) {
  Pedido_Tracking.getTiempoEntregaPedido(
    req.params.idPedido,
    function (err, result) {
      if (!err) {
        return res.json(result);
      } else {
        return res.send(err); // 500 error
      }
    }
  );
};

// Top 10 productos mas vendidos distribuidor
exports.getTop10ProdDistMes = function (req, res) {
  Pedido.getTop10ProdDistMes(req.params, function (err, result) {
    if (!err) {
      return res.json(result);
    } else {
      return res.send(err); // 500 error
    }
  });
};
// Top 10 productos mas vendidos distribuidor
exports.getTop10ProdDistMesApp = function (req, res) {
  const header = req.header("Authorization") || "";
  const token = header.split(" ")[1];
  if (!token) {
    return res.status(401).json({ message: "Token no proporcionado" });
  }
  const payload = jwt.verify(token, config.secret);
  if (!payload.usuario.distribuidor || !payload.usuario._id) {
    return res.status(401).json({ success: true, msj: 'Tienes que tener distribuidor asociado' });
  }
  Pedido.getTop10ProdDistMesApp(payload.usuario.distribuidor._id, function (err, result) {
    if (!err) {
      return res.status(200).json(result);
    } else {
      return res.status(403).send(err); // 500 error
    }
  });
};
/** Traer un producto por id
 *  @obj id del pedido a filtrar
 */
async function getTrackingPorEstado(obj) {
  return new Promise((success) => {
    Pedido_Tracking.get(obj, function (err, result) {
      success(result);
    });
  });
}

/** Traer codigos de descuento por id
 *  @obj id del punto de entrega
 */
async function getCodigoDescuentoById(obj) {
  return new Promise((success) => {
    Codigo_Generado.getCodigoGeneradoById(obj, function (err, result) {
      success(result);
    });
  });
}

/** Traer codigos de descuento por id
 *  @obj id del punto de entrega
 */
async function filterPuntosGanadosByPedido(obj) {
  return new Promise((success) => {
    Puntos_ganados_establecimiento.getfilterPuntosByPedido(
      obj,
      function (err, result) {
        success(result);
      }
    );
  });
}

exports.
  detallePedidoAppDist = async function (req, res) {
    const header = req.header("Authorization") || "";
    const token = header.split(" ")[1];
    if (!token) {
      return res.status(401).json({ message: "Token no proporcionado" });
    }
    Pedido.getPedidoDistApp(
      req.params.idPed,
      function (err, result) {
        if (!err) {
          let data_estados = [
            "Aprobado Interno",
            "Aprobado Externo",
            "Alistamiento",
            "Despachado",
            "Entregado",
          ]
          for (let resp of result) {

            const index = data_estados.findIndex(
              (x) =>
                x === resp.estado
            );
            if (index >= 0) {
              let estados_act
              if (data_estados[index + 1] === 'Aprobado Interno') {
                estados_act = { estado: data_estados[index + 1], slug: 'Pendiente' }
              } else if (data_estados[index + 1] === 'Aprobado Externo') {
                estados_act = { estado: data_estados[index + 1], slug: 'Aprobado' }
              } else {
                estados_act = { estado: data_estados[index + 1], slug: data_estados[index + 1] }
              }
              resp.proximo_estado = estados_act;
              if (index + 1 > 7) {
                resp.mostrar_estado_proximo = false;
              }
            }
            for (let vinc of resp.dataComplete) {
              if (vinc.distribuidor.equals(resp.id_distribuidor)) {
                resp.convenio = vinc.convenio
                resp.cartera = vinc.cartera
                resp.pazysalvo = vinc.pazysalvo

                for (let trab of vinc.info_trabajador) {
                  resp.equipo_comercial += trab.nombre + ' | '
                }
              }
            }
            delete resp.dataComplete
          }
          return res.json(result);
        } else {
          return res.send(err); // 500 error
        }
      }
    );
  };
exports.detallePedidoAppDistIndicadores = async function (req, res) {
  const header = req.header("Authorization") || "";
  const token = header.split(" ")[1];
  if (!token) {
    return res.status(401).json({ message: "Token no proporcionado" });
  }
  try {
    const payload = jwt.verify(token, config.secret);
    let idDist
    if (!payload.usuario.distribuidor || !payload.usuario._id) {
      return res.status(401).json({ success: true, msj: 'Tienes que tener distribuidor asociado' });
    }
    if (!payload.usuario.distribuidor._id) {
      idDist = payload.usuario.distribuidor
    } else {
      idDist = payload.usuario.distribuidor._id

    }
    // Obtener la fecha actual
    let now = moment();

    // Obtener el primer día del mes actual
    let firstDayOfMonth = now.startOf('month').format('YYYY-MM-DD');

    // Obtener el último día del mes actual
    let lastDayOfMonth = now.endOf('month').format('YYYY-MM-DD');

    Pedido.getPedidoDistAppIndicadores(
      {
        distribuidor: idDist,
        inicio: firstDayOfMonth,
        fin: lastDayOfMonth,
      },
      function (err, result) {
        if (!err) {
          let estadosReales = [
            {
              _id: 'Pendientes',
              total_pedido: 0,
              cantidad: 0
            },
            {
              _id: 'Aprobados',
              total_pedido: 0,
              cantidad: 0
            },
            {
              _id: 'Alistando',
              total_pedido: 0,
              cantidad: 0
            },
            {
              _id: 'Despachados',
              total_pedido: 0,
              cantidad: 0
            },
            {
              _id: 'Entregados',
              total_pedido: 0,
              cantidad: 0
            },
            {
              _id: 'Cancelados',
              total_pedido: 0,
              cantidad: 0
            }
          ]
          for (let indicador of result) {
            switch (indicador._id) {
              case 'Pendiente':
                estadosReales[0].total_pedido = estadosReales[0].total_pedido + indicador.total_pedido
                //indicador._id = 'Pendiente';
                break;

              case 'Aprobado Interno':
                //indicador._id  = 'Pendiente';
                estadosReales[0].total_pedido = estadosReales[0].total_pedido + indicador.total_pedido
                estadosReales[0].cantidad = estadosReales[0].cantidad + indicador.cantidad
                break;
              case 'Aprobado Externo':
                //indicador._id  = 'Aprobado';
                estadosReales[1].total_pedido = estadosReales[1].total_pedido + indicador.total_pedido
                estadosReales[1].cantidad = estadosReales[1].cantidad + indicador.cantidad
                break;
              case 'Alistamiento':
                //indicador._id  = 'Alistamiento';
                estadosReales[2].total_pedido = estadosReales[2].total_pedido + indicador.total_pedido
                estadosReales[2].cantidad = estadosReales[2].cantidad + indicador.cantidad
                break;
              case 'Despachado':
                //indicador._id  = 'Despachado';
                estadosReales[3].total_pedido = estadosReales[3].total_pedido + indicador.total_pedido
                estadosReales[3].cantidad = estadosReales[3].cantidad + indicador.cantidad
                break;
              case 'Facturado':
                //indicador._id  = 'Facturado';
                break;
              case 'Entregado':
                //indicador._id= 'Entregado';
                estadosReales[4].total_pedido = estadosReales[4].total_pedido + indicador.total_pedido
                estadosReales[4].cantidad = estadosReales[4].cantidad + indicador.cantidad
                break;
              case 'Recibido':
                //indicador._id  = 'Recibido';
                estadosReales[4].total_pedido = estadosReales[4].total_pedido + indicador.total_pedido
                estadosReales[4].cantidad = estadosReales[4].cantidad + indicador.cantidad
                break;
              case 'Calificado':
                //indicador._id  = 'Calificado';
                estadosReales[4].total_pedido = estadosReales[4].total_pedido + indicador.total_pedido
                estadosReales[4].cantidad = estadosReales[4].cantidad + indicador.cantidad
                break;
              case 'Cancelado por horeca':
                //indicador._id  = 'Cancelado por cliente';
                estadosReales[5].total_pedido = estadosReales[5].total_pedido + indicador.total_pedido
                estadosReales[5].cantidad = estadosReales[5].cantidad + indicador.cantidad
                break;
              case 'Cancelado por distribuidor':
                //indicador._id  = 'Cancelado por distribuidor';
                estadosReales[5].total_pedido = estadosReales[5].total_pedido + indicador.total_pedido
                estadosReales[5].cantidad = estadosReales[5].cantidad + indicador.cantidad
                break;
              case 'Rechazado':
                //indicador._id  = 'Rechazado';
                break;
            }
          }
          let respuesta = {
            indicadores: estadosReales,
            estados: {
              curso: [
                { id: 'pendientes', estado: 'Pendientes' },
                { id: 'aprobados', estado: 'Aprobados' },
                { id: 'alistamiento', estado: 'Alistamiento' },
                { id: 'despachado', estado: 'Despachado' },
              ],
              historico: [
                { id: 'entregados', estado: 'Entregados' },
                { id: 'recibido', estado: 'Recibidos' },
                { id: 'calificados', estado: 'Calificados' },
                { id: 'cancelados', estado: 'Cancelados' },
              ],
              all: [
                { id: 'Aprobado Interno', estado: 'Pendiente' },
                { id: 'Aprobado Externo', estado: 'Aprobado' },
                { id: 'Alistamiento', estado: 'Alistamiento' },
                { id: 'Despachado', estado: 'Despachado' },
                { id: 'Entregado', estado: 'Entregado' },
                // @Deprecated('aprobado quitar por el cliente')
                // {id:'Recibido',estado: 'Recibido'},
                { id: 'Cancelado por distribuidor', estado: 'Cancelado' },
              ]
            }
          }
          if (req.query && req.query.new) {
            return res.status(200).json(respuesta);
          } else {
            return res.status(200).json(estadosReales);
          }
        } else {
          return res.status(203).send(err); // 500 error
        }
      }
    );
  } catch (error) {
    return res.status(500).json({
      success: false,
      message: "Error al intentar consultar",
      data: error,
    });
  }
};
exports.cancelarPedidoDist = async function (req, res) {
  const header = req.header("Authorization") || "";
  const token = header.split(" ")[1];
  if (!token) {
    return res.status(401).json({ message: "Token no proporcionado" });
  }
  Pedido.updateById(
    req.params.idPed,
    { estado: 'Cancelado por distribuidor' },
    function (err, result) {
      if (!err) {
        return res.status(200).json({
          succes: true,
          msj: 'Cancelado con éxito'
        });
      } else {
        return res.status(203).send(err); // 500 error
      }
    }
  );
};
exports.actPedidoDist = async function (req, res) {
  const header = req.header("Authorization") || "";
  const token = header.split(" ")[1];
  if (!token) {
    return res.status(401).json({ message: "Token no proporcionado" });
  }
  Pedido.updateById(
    req.params.idPed,
    { estado: req.body.estado },
    async function (err, result) {
      if (!err) {
        let tra = await Pedido_Tracking.find({ pedido: new ObjectId(req.params.idPed) });
        let postTraking = tra.length - 1
        //Construimos el nuevo objeto para crear nuevo traking
        let data = {
          pedido: req.params.idPed,
          estado_anterior: tra[postTraking].estado_nuevo,
          estado_nuevo: req.body.estado,
          usuario: tra[postTraking].usuario,
        };
        let updateRep = {
          estaAntTraking: tra[postTraking].estado_nuevo,
          estaActTraking: req.body.estado,
        };
        let updateReporte = await ReportePed.updateMany(
          { idPedido: req.params.idPed },
          { $set: updateRep },
          { new: true }
        );
        let tracking = await createPedidoTracking({ data: data });
        return res.status(200).json({
          succes: true,
          msj: 'Actualizado con éxito'
        });
      } else {
        return res.status(203).send(err); // 500 error
      }
    }
  );
};
exports.detallePedidoAppDistIndicadoresAll = async function (req, res) {
  const header = req.header("Authorization") || "";
  const token = header.split(" ")[1];
  if (!token) {
    return res.status(401).json({ message: "Token no proporcionado" });
  }
  try {
    const payload = jwt.verify(token, config.secret);
    if (!payload.usuario.distribuidor || !payload.usuario._id) {
      return res.status(401).json({ success: true, msj: 'Tienes que tener distribuidor asociado' });
    }
    Pedido.getPedidoDistAppIndicadoresAll(
      payload.usuario.distribuidor._id,
      function (err, result) {
        for (let ind of result) {
          if (ind._id === "Aprobado Interno") {
            ind._id = 'Pendiente'
          }
          if (ind._id === "Aprobado Externo") {
            ind._id = 'Aprobado'
          }
        }
        if (!err) {
          return res.status(200).json(result);
        } else {
          return res.status(203).send(err); // 500 error
        }
      }
    );
  } catch (error) {
    return res.status(500).json({
      success: false,
      message: "Error al intentar consultar",
      data: error,
    });
  }
};
exports.valorMinimoPedido = async function (req, res) {
  const header = req.header("Authorization") || "";
  const token = header.split(" ")[1];
  if (!token) {
    return res.status(401).json({ message: "Token no proporcionado" });
  }
  try {
    const payload = jwt.verify(token, config.secret);
    if (!payload.usuario.distribuidor || !payload.usuario._id) {
      return res.status(401).json({ success: true, msj: 'Tienes que tener distribuidor asociado' });
    }
    Distribuidor.valorMinimo(
      payload.usuario.distribuidor._id,
      function (err, result) {
        if (!err) {
          return res.status(200).json(result[0]);
        } else {
          return res.status(203).send(err); // 500 error
        }
      }
    );
  } catch (error) {
    return res.status(500).json({
      success: false,
      message: "Error al intentar consultar",
      data: error,
    });
  }
};
exports.getGraficoBarsDistPedidos = async function (req, res) {
  const header = req.header("Authorization") || "";
  const token = header.split(" ")[1];
  if (!token) {
    return res.status(401).json({ message: "Token no proporcionado" });
  }
  try {
    const payload = jwt.verify(token, config.secret);
    if (!payload.usuario.distribuidor || !payload.usuario._id) {
      return res.status(401).json({ success: true, msj: 'Tienes que tener distribuidor asociado' });
    }
    Pedido.getGraficoBarsDistPedidos(
      { inicio: req.params.inicio, fin: req.params.fin, distribuidor: payload.usuario.distribuidor._id },
      function (err, result) {
        if (!err) {

          // Mostrar el resultado
          return res.status(200).json(result);
        } else {
          return res.status(203).send(err); // 500 error
        }
      }
    );
  } catch (error) {
    return res.status(500).json({
      success: false,
      message: "Error al intentar consultar",
      data: error,
    });
  }
};


exports.pedidoUtility = async function (req, res) {
  const header = req.header("Authorization") || "";
  const token = header.split(" ")[1];
  if (!token) {
    return res.status(401).json({ message: "Token no proporcionado" });
  }
  try {
    const payload = jwt.verify(token, config.secret);
    if (!payload.usuario.distribuidor || !payload.usuario._id) {
      return res.status(401).json({ success: true, msj: 'Tienes que tener distribuidor asociado' });
    }
    Pedido.pedidoUtility(
      req.params.id,
      function (err, result) {
        if (!err) {
          return res.status(200).json(result);
        } else {
          return res.status(203).send(err); // 500 error
        }
      }
    );
  } catch (error) {
    return res.status(500).json({
      success: false,
      message: "Error al intentar consultar",
      data: error,
    });
  }
};
/**
 * Metodo para guardar sugerido, crear traking, y sacar calculo de puntos ganados y redimidos.
 */
exports.pedidoSugNewApi = async function (req, res) {
  const header = req.header("Authorization") || "";
  const token = header.split(" ")[1];
  if (!token) {
    return res.status(401).json({ message: "Token no proporcionado" });
  }
  const payload = jwt.verify(token, config.secret);
  if (!payload.usuario.distribuidor || !payload.usuario._id) {
    return res.status(401).json({ success: true, msj: 'Tienes que tener distribuidor asociado' });
  }
  if (req.body.estado !== 'Sugerido' && req.body.estado !== 'Aprobado Interno') {
    return res.status(401).json({ success: true, msj: 'Debes de asignar un estado válido' });
  }
  const session = await mongoose.startSession();
  try {
    await session.withTransaction(async () => {
      //INICIA METODO CREAR pedidos
      const puntoEntregaId = req.body.punto_entrega;
      const data_punto_e = await puntoEntregaData.findOne({ _id: puntoEntregaId });
      const data_establecimiento = await horeca_user.findOne({ _id: req.body.usuario_horeca });
      if (!data_establecimiento) {
        throw new Error(`El usuario establecimiento no fue encontrado`);
      }
      const idss = req.body.productos_pedido.map(doc => doc.product);
      // Consulta múltiple para obtener los productos y su inventario
      const productos = await Producto.find({ _id: { $in: idss } }).session(session);

      // Validación de inventario
      for (const producto of productos) {
        const pedidoProducto = req.body.productos_pedido.find(p => p.product.toString() === producto._id.toString());
        // Realizar verificacion cuando el producto sea bajo pedido no entre en la siguiente condicion.
        if (producto.precios[0].inventario_unidad < pedidoProducto.unidad) {
          throw new Error(`Inventario insuficiente para el producto ${producto.nombre}.`);
        }
        if (producto.estadoActualizacion != 'Aceptado') {
          throw new Error(`El producto ${producto.nombre} no se encuentra disponible.`);
        }
        // Actualizar el inventario
        if (req.body.estado !== 'Sugerido') {
          await Producto.updateOne({ _id: producto._id }, { $inc: { "precios.0.inventario_unidad": -pedidoProducto.unidad } }, { session });
        }
      }
      for (let prod of req.body.productos_pedido) {
        //validar aca.
        prod.product = new ObjectId(prod.product)
      }
      // Si la dist del producto esta inactivo, mostrar modal para indicar que el distribuidor esta inactivo. 
      let insert_data_pedido = {
        id_pedido: Date.now(),
        estado: req.body.estado,
        usuario_horeca: new ObjectId(req.body.usuario_horeca),
        metodo_pago: req.body.metodo_pago || '',
        distribuidor: new ObjectId(payload.usuario.distribuidor._id),
        trabajador: new ObjectId(payload.usuario._id),
        punto_entrega: new ObjectId(req.body.punto_entrega),
        ciudad: data_punto_e.ciudad,
        direccion: data_punto_e.direccion,
        total_pedido: req.body.total_pedido,
        subtotal_pedido: req.body.subtotal_pedido,
        descuento_pedido: req.body.descuento_pedido,
        puntos_ganados: req.body.puntos_ganados,
        tiempo_tracking_hora: new Date(),
        fecha: new Date(),
        createdAt: new Date(),
        tiempo_estimado_entrega: '22 - 24 horas',
        //codigo_descuento: req.body.codigo_descuento || [],
        precioEspecial: req.body.precioEspecial || [],
        productos: req.body.productos_pedido
      };
      // Inserta el documento en la colección Pedido
      const insertResult = await Pedido.collection.insertOne(insert_data_pedido, { session });
      if (!insertResult || !insertResult.insertedId) {
        throw new Error('Failed to insert the pedido');
      }
      const pedidoId = insertResult.insertedId;
      // FIN METODO CREAR PEDIDO
      // INICIO METODO CREAR pedido_trackings
      let data = {
        pedido: pedidoId,
        estado_anterior: req.body.estado,
        estado_nuevo: req.body.estado,
        usuario: payload.usuario._id,
        createdAt: new Date(),
      };
      // Inserta el documento en la colección Pedido_Tracking
      const insert_traking = await Pedido_Tracking.collection.insertOne(data, { session });
      if (!insert_traking || !insert_traking.insertedId) {
        throw new Error('Failed to insert the traking');
      }
      const idTraking = insert_traking.insertedId;
      //FIN METODO TRAKING
      //INICIA METODO PARA CREAR Puntos_ganados_establecimiento
      if (req.body.puntos_ganados > 0) {
        data = {
          punto_entrega: req.body.punto_entrega,
          pedido: pedidoId,
          puntos_ganados: req.body.puntos_ganados,
          movimiento: "Aplica",
          fecha: new Date(),
          createdAt: new Date(),
        };
        // puntos_ganados = await createPuntosGanados({ data: data });
        const insert_puntos_ganados = await Puntos_ganados_establecimiento.collection.insertOne(data, { session });
        if (!insert_puntos_ganados || !insert_puntos_ganados.insertedId) {
          throw new Error('Failed to insert the puntos gandos');
        }
      }

      // Consulta múltiple
      const ids = req.body.productos_pedido.map(doc => doc.product);
      const documentosEncontrados = await Producto.find({ _id: { $in: ids } }).session(session);
      const historicoPed = await createHistoricoPedidosDistribuidorObject(
        documentosEncontrados,
        insert_data_pedido,
        idTraking,
        pedidoId,
        data_punto_e,
        data_establecimiento
      );
      const insert_historico = await ReportePed.insertMany(historicoPed, { session });
    });
    res.status(200).send({ success: true, message: 'Pedido generado' });
  } catch (error) {
    res.status(500).send({ success: false, message: 'Ha fallado la creacion del pedido', error: error.message });
  } finally {
    session.endSession();
  }
};
/**
 * Metodo para guardar sugerido, crear traking, y sacar calculo de puntos ganados y redimidos.
 */
exports.getChatsActivosByPuntoNewApp = async function (req, res) {
  const header = req.header("Authorization") || "";
  const token = header.split(" ")[1];
  if (!token) {
    return res.status(401).json({ message: "Token no proporcionado" });
  }
  const payload = jwt.verify(token, config.secret);
  if (!payload.usuario.distribuidor || !payload.usuario._id) {
    return res.status(401).json({ success: true, msj: 'Tienes que tener distribuidor asociado' });
  }
  try {
    const { idPunto, page } = req.params;
    const totalTrabajadores = await Pedido.countDocuments(
      {
        distribuidor: new ObjectId(payload.usuario.distribuidor._id)
      }
    );
    const skip = (page - 1) * 30;
    const totalPaginas = Math.ceil(totalTrabajadores / 30);

    const aggregationQuery = [
      {
        $match: {
          distribuidor: new ObjectId(payload.usuario.distribuidor._id)
        }
      },
      {
        $project: {
          id: "$_id",
          distribuidor: "$distribuidor",
          id_pedido: "$id_pedido",
          punto_entrega: "$punto_entrega",
          usuario_horeca: "$usuario_horeca"
        }
      },
      {
        $lookup: {
          from: "punto_entregas",
          localField: "punto_entrega", //Nombre del campo de la coleccion actual
          foreignField: "_id", //Nombre del campo de la coleccion a relacionar
          as: "dataPuntoEn", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
      {
        $lookup: {
          from: "usuario_horecas",
          localField: "usuario_horeca", //Nombre del campo de la coleccion actual
          foreignField: "_id", //Nombre del campo de la coleccion a relacionar
          as: "dataPuntoEsta", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
      {
        $lookup: {
          from: "mensajes",
          let: { pedidoId: "$_id" },
          pipeline: [
            { $match: { $expr: { $eq: ["$pedido", "$$pedidoId"] } } },
            { $unwind: "$conversacion" },
            { $sort: { "conversacion.leido": 1, "conversacion.createdAt": -1 } }
          ],
          as: "chats"
        }
      },
      {
        $unwind: {
          path: "$chats",
          preserveNullAndEmptyArrays: true // Esto asegura que se incluyan documentos sin chats
        }
      },
      {
        $group: {
          _id: "$_id", // Agrupar por el ID original del pedido
          id: { $first: "$id" },
          distribuidor: { $first: "$distribuidor" },
          id_pedido: { $first: "$id_pedido" },
          punto_entrega: { $first: "$punto_entrega" },
          nombre: { $first: "$dataPuntoEn.nombre" },
          logo: { $first: "$dataPuntoEsta.logo" },
          usuario_horeca: { $first: "$usuario_horeca" },
          chats: { $push: "$chats" },
          noleidosCount: {
            $sum: { $cond: [{ $eq: ["$chats.conversacion.leido", false] }, 1, 0] }
          },
          ultimo_mensaje: {
            $first: {
              $cond: [
                { $isArray: "$chats" },
                { $arrayElemAt: ["$chats.conversacion.mensaje", 0] },
                ""
              ]
            }
          },
          usuario_mensaje: {
            $first: {
              $cond: [
                { $isArray: "$chats" },
                { $arrayElemAt: ["$chats.conversacion.usuario", 0] },
                ""
              ]
            }
          },
          usuario_mensaje_tipo: {
            $first: {
              $cond: [
                { $isArray: "$chats" },
                { $arrayElemAt: ["$chats.conversacion.tipo", 0] },
                ""
              ]
            }
          },
          mensaje_tiempo: {
            $first: {
              $cond: [
                { $isArray: "$chats" },
                { $arrayElemAt: ["$chats.conversacion.tiempo", 0] },
                ""
              ]
            }
          }
        }
      },
      {
        $addFields: {
          hasChats: { $cond: { if: { $gt: [{ $size: "$chats" }, 0] }, then: true, else: false } }
        }
      },
      // Ordenar primero los pedidos con chats y luego los sin chats
      {
        $sort: {
          hasChats: -1, // Primero los que tienen chats (true), luego los que no tienen (false)
          "noleidosCount": -1 // Ordenar por tiempo de mensaje (último mensaje)
        }
      },
      { $skip: skip },
      { $limit: 30 }
    ];

    const pedidos_conversaciones = await Pedido.aggregate(aggregationQuery);


    if (pedidos_conversaciones) {
      return res.status(200).json({
        succes: true,
        chats: pedidos_conversaciones,
        paginaActual: page,
        totalPaginas: totalPaginas
      });
    } else {
      return res.status(204).json({
        registered: false,
        message: "Error al obtener notificaciones",
      });
    }
  } catch (error) {
    res.status(500).send({ success: false, message: 'Ha fallado la creacion del pedido', error: error.message });
  }
};
exports.reportePedidoPFALL = async function (req, res) {
  const queryadd = [
    {
      $match: {
        usuario_horeca: new ObjectId(req.params.horeca)
      }
    },
    {
      $lookup: {
        from: 'reportepedidos', //Nombre de la colecccion a relacionar
        localField: "_id", //Nombre del campo de la coleccion actual
        foreignField: "idPedido", //Nombre del campo de la coleccion a relacionar
        as: "dataReporte", //Nombre del campo donde se insertara todos los documentos relacionados
      },
    },
    {
      $lookup: {
        from: 'productos', //Nombre de la colecccion a relacionar
        localField: "productos.product", //Nombre del campo de la coleccion actual
        foreignField: "_id", //Nombre del campo de la coleccion a relacionar
        as: "dataProd", //Nombre del campo donde se insertara todos los documentos relacionados
      },
    },
    {
      $project: {
        id: "$_id",
        id_pedido: "$id_pedido",
        dataReporte: "$dataReporte",
        dataProd: "$dataProd",
        punto_entrega: "$punto_entrega",
        estado: "$estado",
        //productos:"$productos",
        puntos_ganados: "$puntos_ganados",
        puntos_redimidos: "$puntos_redimidos",
        fechaPedido: "$createdAt",
      }
    },
    {
      $sort: { fechaPedido: -1 }
    }
  ];

  const responseQuery = await Pedido.aggregate(queryadd);

  if (responseQuery) {
    return res.status(200).json({
      respuesta: responseQuery,
    });
  } else {
    return res.status(204).json({
      message: "Error al obtener notificaciones",
    });
  }
};
exports.actualizarmany = async function (req, res) {
  ReportePed.updateMany(
    { idPedido: req.params.idPed },
    { puntosGanados: parseInt(req.params.puntos) },
    function (err, docs) {
      if (err) {
        res.status(500).json({
          message: "Error al actualizando notificaciones",
        });
      } else {
        return res.status(200).json({
          succes: true,
          message: "Actualizado correctamente",
          docs: docs

        });
      }
    }
  );
};
exports.actulizarPedidoAppDist = async function (req, res) {
  const header = req.header("Authorization") || "";
  const token = header.split(" ")[1];
  if (!token) {
    return res.status(401).json({ message: "Token no proporcionado" });
  }

  let payload;
  try {
    payload = jwt.verify(token, config.secret);
  } catch (error) {
    return res.status(401).json({ message: "Token inválido" });
  }

  if (!payload.usuario.distribuidor || !payload.usuario._id) {
    return res.status(401).json({ success: true, msj: 'Tienes que tener distribuidor asociado' });
  }

  const session = await mongoose.startSession();

  try {
    await session.withTransaction(async () => {
      const idDistribuidor = payload.usuario.distribuidor._id;
      const idPedido = new ObjectId(req.body.pedido_id);
      const productosActualizados = req.body.productos;
      const pedido = await Pedido.findOne({ _id: idPedido }).session(session);
      if (!pedido) {
        throw new Error('El pedido no existe');
      }

      const promises = [];
      // Comparar productos y ajustar inventario
      for (const productoActualizado of productosActualizados) {
        const productoPedido = pedido.productos.find(p => p.product.toString() === productoActualizado.product_id.toString());
        if (productoPedido) {
          const diferencia = productoPedido.unidad - productoActualizado.cantidad;
          const producto = await Producto.findById(productoPedido.product).session(session);
          if (diferencia !== 0) {
            if (diferencia > 0) {
              producto.precios[0].inventario_unidad += diferencia;
            } else if (producto.precios[0].inventario_unidad >= -diferencia) {
              producto.precios[0].inventario_unidad += diferencia;
            } else {
              throw new Error('Stock insuficiente para el producto ' + producto.nombre);
            }

            promises.push(producto.save({ session }));
          }
        } else {

          throw new Error('Error al encontrar el producto en pedido');
        }
      }

      // Crear un mapa para una búsqueda más rápida de productos
      const mapaProductosA = new Map(productosActualizados.map(producto => [producto.product_id, producto.cantidad]));

      pedido.productos = await Promise.all(pedido.productos.map(async objetoB => {
        const cantidad = mapaProductosA.get(objetoB.product.toString());
        if (cantidad !== undefined) {
          objetoB.unidad = cantidad;
          const idPedidoFiltro = pedido._id;
          const productoFiltro = objetoB.product;
          const reportePedido = await ReportePed.findOne({
            idPedido: new ObjectId(idPedidoFiltro),
            productoId: new ObjectId(productoFiltro),
          }).session(session);
          if (reportePedido) {
            const productoActualizar = await Producto.findById(reportePedido.productoId).session(session);
            if (productoActualizar) {
              reportePedido.unidadesCompradas = objetoB.unidad;
              reportePedido.costoProductos = objetoB.precio_original;
              reportePedido.puntos_ft_unidad = productoActualizar.precios[0].puntos_ft_unidad;
              reportePedido.puntos_ft_ganados = productoActualizar.precios[0].puntos_ft_unidad * objetoB.unidad;
              reportePedido.detalleProducto.precios = productoActualizar.precios[0];
              promises.push(reportePedido.save({ session }));
            } else {
              throw new Error('Error al consultar el producto para actualizar los reportes');
            }
          } else {

            throw new Error('Error al consultar el pedido para actualizar los reportes');
          }
          return objetoB; // Mantener el objeto

        } else {
          // Logica para devolver inventario de productos eliminados
          const idPedidoFiltro = pedido._id;
          const productoFiltro = objetoB.product;

          const reportePedido = await ReportePed.findOne({
            idPedido: new ObjectId(idPedidoFiltro),
            productoId: new ObjectId(productoFiltro),
          }).session(session);
          if (reportePedido) {
            const productoActualizar = await Producto.findById(reportePedido.productoId).session(session);
            if (productoActualizar) {
              productoActualizar.precios[0].inventario_unidad += objetoB.unidad;
              promises.push(productoActualizar.save({ session }));
            } else {
              throw new Error('Error al encontrar el producto para devolver inventario');
            }
            promises.push(reportePedido.remove({ session }));
          } else {
            throw new Error('Error al encontrar el reporte del pedido');
          }

          return null; // Eliminar el objeto
        }
      }).filter(objeto => objeto !== null));
      pedido.productos = pedido.productos.filter(objeto => objeto !== null);
      // Recalcular los totales del pedido
      let discont = pedido.subtotal_pedido - pedido.total_pedido;
      let newTotal = 0;
      let newSubTotal = 0;
      let deleteItem = 0
      for (let pedidofinal of pedido.productos) {
        if (pedidofinal) {
          newTotal += (pedidofinal.unidad * pedidofinal.precio_original) - discont;
          newSubTotal += pedidofinal.unidad * pedidofinal.precio_original;
        } else {
          delete pedido.productos[deleteItem]
        }
        deleteItem++
      }
      pedido.subtotal_pedido = newSubTotal;
      pedido.total_pedido = newTotal;



      const result = await ReportePed.updateMany(
        { idPedido: new ObjectId(pedido._id) },
        { totalCompra: newTotal, subtotalCompra: newSubTotal },
        { session }
      );
      promises.push(pedido.save({ session }));
      // Esperar a que todas las promesas se resuelvan
      await Promise.all(promises);
    });

    res.status(200).send({ success: true, message: 'Pedido actualizado exitosamente' });
  } catch (error) {
    res.status(500).send({ success: false, message: 'Ha fallado la actualización del pedido', error: error.message });
  } finally {
    session.endSession();
  }
};
exports.actulizarPedidoNew = async function (req, res) {
  const header = req.header("Authorization") || "";
  const token = header.split(" ")[1];
  if (!token) {
    return res.status(401).json({ message: "Token no proporcionado" });
  }

  let payload;
  try {
    payload = jwt.verify(token, config.secret);
  } catch (error) {
    return res.status(401).json({ message: "Token inválido" });
  }
  const session = await mongoose.startSession();

  try {
    await session.withTransaction(async () => {
      //const idDistribuidor = payload.usuario.distribuidor._id;
      const idPedido = new ObjectId(req.body.pedido_id);
      const products_update = req.body.productos;
      let copy_producto_origin
      //pedido original
      const pedido = await Pedido.findOne({ _id: idPedido }).session(session);
      copy_producto_origin = await Pedido.findOne({ _id: idPedido }).session(session);
      if (!pedido) {
        throw new Error('El pedido no existe');
      }
      const productIdsArreglo2 = products_update.map(item => new ObjectId(item.product_id));
      // Filtrar y eliminar los productos que han sido quitados del carrito
      pedido.productos = pedido.productos.filter(producto =>
        productIdsArreglo2.some(productId => productId.equals(producto.product))
      );
      //Promesas a ejecutar
      const promises = [];
      //Reiniciando todos los valores principales de costos y puntos.
      pedido.total_pedido = 0
      pedido.subtotal_pedido = 0
      pedido.descuento_pedido = 0
      pedido.puntos_ganados = 0
      // recorre los productos del nuevo carro editado.
      for (const prod of products_update) {
        //Busca el producto original para obtener la data
        const producto_original = await Producto.findById(prod.product_id).session(session);
        //Si el producto originalmente fue borrado da un error.
        if (!producto_original) {
          throw new Error('Error al obtener producto:' + prod.product_id);
        }
        //busca precio en precios especiales
        const productoRDescuento = await descuentosPunto.findOne({ idPunto: pedido.punto_entrega.toString(), 'productosDescuento.productId': prod.product_id.toString() }).session(session);
        //busca los productos en el pedido original, si no los encuentra los agrega al array
        const productoPedido = await pedido.productos.find(p => p.product.toString() === prod.product_id.toString());
        let producto_ped
        if (!productoPedido) {
          producto_ped = {
            product: new ObjectId(producto_original._id),
            caja: producto_original.precios[0].und_x_caja,
            unidad: prod.cantidad,
            puntos_ft_unidad: producto_original.precios[0].puntos_ft_unidad,
            puntos_ft_caja: 0,
            data_precioProducto: producto_original.precios,
            precio_original: producto_original.precios[0].precio_unidad,
            und_x_caja: producto_original.precios[0].und_x_caja,
            precio_caja: producto_original.precios[0].precio_caja,
            porcentaje_descuento: producto_original.prodDescuento,
            precioEspecial: producto_original.precio_descuento,
          }
          pedido.productos.push(producto_ped);
          //Calcula diferencias para validar inventario y restar unidades
          const diferencia = producto_original.precios[0].inventario_unidad - prod.cantidad;
          if (diferencia > 0) {
            producto_original.precios[0].inventario_unidad = diferencia;
          } else {
            throw new Error('Stock insuficiente para el producto ' + producto_original.nombre);
          }
          promises.push(producto_original.save({ session }));
        } else {
          producto_ped = productoPedido;
          //Calcula diferencias para validar inventario y devolver o restar unidades segun diferencia.
          for (let prodAnt of copy_producto_origin.productos) {
            if (prodAnt.product.toString() === prod.product_id.toString()) {
              if (prodAnt.unidad > prod.cantidad) {
                let dif = prodAnt.unidad - prod.cantidad;
                producto_original.precios[0].inventario_unidad += dif;
                promises.push(producto_original.save({ session }));
              }
              if (prodAnt.unidad < prod.cantidad) {
                let dif = prod.cantidad - prodAnt.unidad;
                producto_original.precios[0].inventario_unidad -= dif;
                promises.push(producto_original.save({ session }));
              }
            }
          }
        }
        //Calcula los puntos ganados en caso de que existan.
        if (producto_original.mostrarPF && producto_original.precios[0].puntos_ft_unidad > 0) {
          pedido.puntos_ganados += (prod.cantidad * producto_original.precios[0].puntos_ft_unidad)
        }
        //Asigna la nueva unidad
        producto_ped.unidad = prod.cantidad

      }
      for (let pd of pedido.productos) {
        //Construye de nuevo el subtotal del pedido
        let multiplicacionTotal = pd.unidad * pd.precio_original;
        pedido.subtotal_pedido = pedido.subtotal_pedido + multiplicacionTotal
        pedido.total_pedido = pedido.subtotal_pedido - pedido.descuento_pedido
      }
      //Push de promesa a ejecutar, solucion para colección de pedidos
      promises.push(pedido.save({ session }).then(savedPedido => {
        return savedPedido; // Importante para que la promesa se resuelva con el pedido guardado
      }).catch(error => {
        throw error; // Re-lanza el error para que se maneje en un nivel superior si es necesario
      }));
      //inicio de configuración y edicion para pedidos desde reporte.
      const reportePedido = await ReportePed.find({
        idPedido: new ObjectId(pedido._id),
      }).session(session);
      let model_estructure = reportePedido[0];
      delete model_estructure._id
      for (let prod_inicial of copy_producto_origin.productos) {
        let deleteProd = false
        for (let prod_fin of pedido.productos) {
          if (prod_inicial.product.toString() === prod_fin.product.toString()) {
            deleteProd = true
          }
        }
        if (!deleteProd) {
          //await ReportePed.deleteOne({ productoId: prod_inicial.product, idPedido: new ObjectId(pedido._id) });
          promises.push(ReportePed.deleteOne({ productoId: prod_inicial.product, idPedido: new ObjectId(pedido._id) }, { session })
            .then(result => {
              return result; // Devuelve el resultado del borrado
            })
            .catch(error => {
              throw error; // Re-lanza el error
            }));
        }
      }
      for (let prodPedido of pedido.productos) {
        const reporte = reportePedido.find(r => r.productoId.equals(prodPedido.product));
        if (reporte) {
          // Si encuentra el reporte, actualiza las propiedades
          reporte.unidadesCompradas = prodPedido.unidad;
          reporte.costoProductos = prodPedido.precio_original;
          // Actualización con promises.push
          promises.push(ReportePed.updateOne(
            { _id: reporte._id },
            {
              $set: {
                unidadesCompradas: prodPedido.unidad,
                costoProductos: prodPedido.precio_original,
                totalCompra: pedido.total_pedido,
                subtotalCompra: pedido.subtotal_pedido,
                descuento: pedido.descuento_pedido,
                puntos_ganados: pedido.puntos_ganados
              }
            },
            { session } // Añade la sesión aquí también
          )
            .then(result => {
              return result; // Devuelve el resultado de la actualización
            })
            .catch(error => {
              throw error; // Re-lanza el error
            }));
        } else {

          const producto_reporte = await Producto.findById(prodPedido.product).session(session);
          let punto_ganados = 0
          if (producto_reporte.mostrarPF && producto_reporte.precios[0].puntos_ft_unidad > 0) {
            punto_ganados = producto_reporte.precios[0].puntos_ft_unidad * prodPedido.cantidad
          }
          model_estructure.productoId = new ObjectId(producto_reporte._id);
          model_estructure.categoriaProducto = new ObjectId(producto_reporte);
          model_estructure.lineaProducto = new ObjectId(producto_reporte);
          model_estructure.marcaProducto = new ObjectId(producto_reporte);
          model_estructure.nombreProducto = producto_reporte.nombre;
          model_estructure.codigoFeatProducto = producto_reporte.codigo_ft;
          model_estructure.codigoDistribuidorProducto = producto_reporte.codigo_distribuidor;
          model_estructure.unidadesCompradas = prodPedido.unidad;
          model_estructure.costoProductos = prodPedido.precio_original;
          model_estructure.referencia = '';
          model_estructure.puntos_ft_unidad = producto_reporte.precios[0].puntos_ft_unidad || 0;
          model_estructure.puntos_ft_caja = producto_reporte.precios[0].puntos_ft_caja || 0;
          model_estructure.puntos_ft_ganados = pedido.puntos_ganados
          model_estructure.precio_caja_app = 0;
          model_estructure.detalleProducto = producto_reporte;
          model_estructure.precioEspecial = [];
          model_estructure.totalCompra = pedido.total_pedido;
          model_estructure.subtotalCompra = pedido.subtotal_pedido;
          model_estructure.descuento = pedido.descuento_pedido;
          const cleanedModel = JSON.parse(JSON.stringify(model_estructure)); // Elimina automáticamente propiedades no serializables como funciones
          delete cleanedModel._id;
          delete cleanedModel.__v;
          const result = await ReportePed.create(cleanedModel, { session });
          promises.push(result);
        }
      }
      await Promise.all(promises);
    });
    res.status(200).send({ success: true, message: 'Pedido actualizado exitosamente' });
  } catch (error) {
    res.status(500).send({ success: false, message: 'Ha fallado la actualización del pedido', error: error.message });
  } finally {
    session.endSession();
  }
};
exports.actualizarmasivoerrores = async function (req, res) {

  try {
    // Crear el objeto de actualización
    const update = {
      puntosGanados: 820
    };

    // Realizar la actualización
    const resultado = await ReportePed.updateMany(
      { idPedido: req.params.idPed },
      { $set: update },
      { new: true }
    );

    // Verificar el resultado
    if (resultado.modifiedCount > 0) {
      res.send(`Se actualizaron ${resultado.modifiedCount} documentos.`);

    } else {
      res.send(`Se actualizaron no documentos.`);
    }
  } catch (error) {
    res.send(` ${error}.`);
  }
};