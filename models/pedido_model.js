const { json } = require("body-parser");
const mongoose = require("mongoose");
const ProductosBD = require("./producto_model");
const LineaBD = require("./linea_producto_model");
const MarcaBD = require("./marca_producto_model");
const CategoriaBD = require("./categoria_producto_model");
const MensajeDB = require("./mensajes_model");
const trabajadoresBD = require("./trabajador_model");
const horecaBD = require("./usuario_horeca_model");
const pedidoTrackingBD = require("./pedido_tracking_model");
const momentFe = require("moment");
const moment = require('moment-timezone');
const _ = require('lodash');
const jwt = require("jsonwebtoken");
const config = require("../config/database");
// Schema
var ObjectId = require("mongoose").Types.ObjectId;

const PedidoSchema = mongoose.Schema({
  usuario_horeca: {
    type: mongoose.Schema.Types.ObjectId,
    ref: "Usuario_horeca",
    required: true,
  },
  punto_entrega: {
    type: mongoose.Schema.Types.ObjectId,
    ref: "Punto_entrega",
    required: true,
  },
  trabajador: {
    type: mongoose.Schema.Types.ObjectId,
    ref: "Trabajador",
    required: true,
  },
  distribuidor: {
    type: mongoose.Schema.Types.ObjectId,
    ref: "Distribuidor",
    required: true,
  },
  id_pedido: { type: Number, required: true },
  fecha: { type: Date, required: true },
  estado: {
    type: String,
    required: true,
    default: "Pendiente",
    enum: [
      "Sugerido", //distribuidor al planeador de pedidos
      "Pendiente", //creado pendiente por aprobacion de admin horeca
      "Aprobado Interno", //admin del horeca
      "Aprobado Externo", //distribuidor
      "Alistamiento",
      "Despachado",
      "Facturado", //intermedio opcional del distribuidor
      "Entregado", //lo coloca el distribuidor al momento de la entrega
      "Recibido", // lo confirma el punto de entrega que recibe
      "Calificado", //obcional despues de la entrega
      "Rechazado", //admin del horeca
      "Cancelado por horeca", //admin del horeca
      "Cancelado por distribuidor",
    ],
  }, 
  metodo_pago: { type: String, required: true },
  ciudad: { type: String, required: true },
  direccion: { type: String, required: true },
  total_pedido: { 
    type: Number,
    default: 0,
    set: (v) => typeof v === 'string' ? parseFloat(v.replace(",", ".")) : v,
  },
  subtotal_pedido: { 
    type: Number,
    default: 0,
    set: (v) => typeof v === 'string' ? parseFloat(v.replace(",", ".")) : v,
  },
  descuento_pedido: { type: Number, required: false, default: 0 },
  puntos_ganados: { type: Number, required: false },
  puntos_redimidos: { type: Number, required: false },
  tiempo_estimado_entrega: { type: String, required: true },
  tiempo_tracking_hora: { type: String, required: true },
  pre_factura: { type: String, required: false },
  calificacion: { type: Object, required: false },
  productos: [
    {
      product: {
        type: mongoose.Schema.Types.ObjectId,
        ref: "Producto",
        required: true,
      },
      caja: { type: Number, required: false },
      unidad: { type: Number, required: false },
      referencia: { type: String, required: false },
      categoria: { type: String, required: false },
      linea: { type: String, required: false },
      marca: { type: String, required: false },
      puntos_ft_unidad: { type: Number, required: false },
      puntos_ft_caja: { type: Number, required: false },
      data_precioProducto: [],
      unidad_medida: { type: String, required: false },
      cantidad_medida: { type: Number, required: false },
      precio_original: { type: Number, required: false, default: 0 },
      und_x_caja: { type: Number, required: false },
      precio_caja: { type: Number, required: false, default: 0 },
      porcentaje_descuento: { type: Boolean, required: false },
      precioEspecial: []
    },
  ],
  codigo_descuento: {
    type: [mongoose.Schema.Types.ObjectId],
    ref: "Codigos_descuento_generados",
    required: false,
  },
  createdAt: { type: Date, required: false, default: Date.now },
});
PedidoSchema.statics = {
  get: function (query, callback) {
    this.findOne(query)
      .populate("punto_entrega")
      .populate("codigo_descuento")
      .exec(callback);
  },
  getAll: function (query, callback) {
    this.find(query).sort({ createdAt: -1 }).exec(callback);
  },
  getAllPopulate: function (query, callback) {
    this.find(query)
      .populate({
        path: "productos.product",
        populate: {
          path: "categoria_producto",
          model: "Categoria_producto",
          select: "nombre",
        },
      })
      .populate({
        path: "productos.product",
        populate: {
          path: "marca_producto",
          model: "Marca_producto",
        },
      })
      .exec(callback);
  },
  updateById: function (id, updateData, callback) {
    this.findOneAndUpdate(
      { _id: id },
      { $set: updateData },
      { new: true },
      callback
    );
  },
  removeById: function (removeData, callback) {
    this.findOneAndRemove(removeData, callback);
  },
  removeAll: function (query, callback) {
    this.remove(query).exec(callback);
  },
  create: function (data, callback) {
    const Pedido = new this(data);
    Pedido.save(callback);
  },
  getPedidoUpdate: function (obj, callback) {
    var ObjectId = require("mongoose").Types.ObjectId;
    const query = {
      _id: new ObjectId(obj),
    };
    Pedido.find(query).exec(callback);
  },
  getPedidosByPuntoSimple: function (obj, callback) {
    var ObjectId = require("mongoose").Types.ObjectId;
    const query = {
      punto_entrega: new ObjectId(obj.punto_entrega),
    };
    Pedido.find(query).populate("punto_entrega distribuidor").exec(callback);
  },
  getPedidosByPuntoEntrega: function (obj, callback) {
    var d = new Date();
    var fechaReferencia = d.setMonth(d.getMonth() - 3);
    var ObjectId = require("mongoose").Types.ObjectId;
    const query = {
      punto_entrega: new ObjectId(obj.punto_entrega),
      fecha: {
        $gte: fechaReferencia,
      },
    };
    Pedido.find(query).populate("distribuidor").exec(callback);
  },
  getPedidosByPuntoDist: function (obj, callback) {
    var ObjectId = require("mongoose").Types.ObjectId;
    const query = {
      distribuidor: new ObjectId(obj.distribuidor),
      punto_entrega: new ObjectId(obj.punto_entrega),
      ...(obj.query || {}),
    };
    Pedido.find(query).populate("distribuidor", ["nombre"]).exec(callback);
  },
  getPedidosByPunto: function (obj, callback) {
    var d = new Date();
    var fechaReferencia = d.setMonth(d.getMonth() - 3);
    var ObjectId = require("mongoose").Types.ObjectId;
    const query = {
      usuario_horeca: new ObjectId(obj.usuario_horeca),
      punto_entrega: new ObjectId(obj.punto_entrega),
      fecha: {
        $gte: fechaReferencia,
      },
    };
    Pedido.find(query, callback);
  },
  getPedidosByDistribuidor: function (obj, callback) {
    var d = new Date();
    var fechaReferencia = d.setMonth(d.getMonth() - 3);
    var ObjectId = require("mongoose").Types.ObjectId;
    const query = {
      distribuidor: new ObjectId(obj.distribuidor),
      // fecha: {
      //   $gte: fechaReferencia,
      // },
    };
    Pedido.find(query, callback).populate(
      "trabajador distribuidor punto_entrega usuario_horeca codigo_descuento"
    );
  },
  getListaPedidos: function (obj, callback) {
    var d = new Date();
    var fechaReferencia = d.setMonth(d.getMonth() - 3);
    var ObjectId = require("mongoose").Types.ObjectId;
    const query = {
      distribuidor: new ObjectId(obj.distribuidor),
      // fecha: {
      //   $gte: fechaReferencia,
      // },
    };
    this.aggregate([
      {
        $match: {},
      },
      {
        $lookup: {
          from: "punto_entregas",
          localField: "punto_entrega", //Nombre del campo de la coleccion actual
          foreignField: "_id", //Nombre del campo de la coleccion a relacionar
          as: "data_punto_entrega", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
      {
        $lookup: {
          from: "distribuidors",
          localField: "distribuidor", //Nombre del campo de la coleccion actual
          foreignField: "_id", //Nombre del campo de la coleccion a relacionar
          as: "data_distribuidor", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
      {
        $lookup: {
          from: "codigos_descuento_generados",
          localField: "codigo_descuento", //Nombre del campo de la coleccion actual
          foreignField: "_id", //Nombre del campo de la coleccion a relacionar
          as: "data_codigo_descuento", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
      {
        $lookup: {
          from: "usuario_horecas",
          localField: "usuario_horeca", //Nombre del campo de la coleccion actual
          foreignField: "_id", //Nombre del campo de la coleccion a relacionar
          as: "data_usuario_horeca", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
      {
        $lookup: {
          from: "pedido_trackings",
          localField: "_id", //Nombre del campo de la coleccion actual
          foreignField: "pedido", //Nombre del campo de la coleccion a relacionar
          as: "data_trackings", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
      {
        $addFields: {
          ultimo_tracking: { $last: "$data_trackings" },
        },
      },
      {
        $project: {
          _id: "$_id",
          idPedido: "$id_pedido",
          distribuidor_nombre: {
            $arrayElemAt: ["$data_distribuidor.nombre", 0],
          },
          establecimiento_nombre: {
            $arrayElemAt: ["$data_usuario_horeca.nombre_establecimiento", 0],
          },
          establecimiento_nit: {
            $arrayElemAt: ["$data_usuario_horeca.nit", 0],
          },
          establecimiento_pais: {
            $arrayElemAt: ["$data_usuario_horeca.pais", 0],
          },
          establecimiento_departamento: {
            $arrayElemAt: ["$data_usuario_horeca.departamento", 0],
          },
          establecimiento_ciudad: {
            $arrayElemAt: ["$data_usuario_horeca.ciudad", 0],
          },
          establecimiento_tipo_negocio: {
            $arrayElemAt: ["$data_usuario_horeca.tipo_negocio", 0],
          },
          codigos_descuentos: "$data_codigo_descuento",
          establecimiento_fecha_pedido: { $dateToString: { format: "%Y/%m/%d - %H:%M", date: "$createdAt" } },
          establecimiento_total_pedido: "$total_pedido",
          establecimiento_metodo_pago: "$metodo_pago",
          establecimiento_puntos_ganados: "$puntos_ganados",
          establecimiento_puntos_redimidos: "$puntos_redimidos",
          establecimiento_puntos_redimidos: "$puntos_redimidos",
          establecimiento_descuento_pedido: "$descuento_pedido",
          ultimo_tracking: "$estado",
          fecha_ultimo_tracking: "$ultimo_tracking.createdAt",
        },
      },
    ])
      .sort({ establecimiento_fecha_pedido: -1 })
      .exec(callback);
  },
  /** Total de pedidos pendientes por aprobar de un distribuidor */
  getCountPedidosPendientesByDistribuidor: function (obj, callback) {
    const ObjectId = require("mongoose").Types.ObjectId;
    const query = {
      distribuidor: new ObjectId(obj.distribuidor),
      estado: "Aprobado Interno",
    };
    Pedido.count(query, callback);
  },
  getPedidosByDistribuidorDetallado: function (obj, callback) {
    var ObjectId = require("mongoose").Types.ObjectId;
    const query = {
      distribuidor: new ObjectId(obj.distribuidor),
      fecha: {
        $gte: new Date(10585543224),
        $lte: Date.now(),
      },
    };
    Pedido.find(query, callback);
  },
  getPedidosByTrabajador: function (obj, callback) {
    var ObjectId = require("mongoose").Types.ObjectId;
    const query = {
      trabajador: new ObjectId(obj.trabajador),
    };
    Pedido.count(query, callback);
  },
  getPedidoDetallado: function (query, callback) {
    this.find(query)
      .populate("usuario_horeca punto_entrega distribuidor")
      .populate({
        path: "productos",
        populate: {
          path: "product",
          populate: {
            path: "marca_producto categoria_producto linea_producto precios codigo_organizacion",
          },
        },
      })
      .exec(callback);
  },
  getPedidoProductosDetallados: function (obj, callback) {
    var ObjectId = require("mongoose").Types.ObjectId;
    const query = {
      _id: new ObjectId(obj._id),
    };
    this.find(query)
      .populate("punto_entrega distribuidor codigo_descuento")
      .populate({
        path: "productos",
        populate: {
          path: "product",
        },
      })
      .exec(callback);
  },
  getPedidoProductosDetalladosVolverPedir: function (obj, callback) {
    var ObjectId = require("mongoose").Types.ObjectId;
    const query = {
      _id: new ObjectId(obj._id),
    };
    this.aggregate([
      {
        $match: query,
      },
      {
        $lookup: {
          from: "reportepedidos",
          localField: "_id", //Nombre del campo de la coleccion actual
          foreignField: "idPedido", //Nombre del campo de la coleccion a relacionar
          as: "historicoPedido", //Nombre del campo donde se insertara todos los documentos relacionados
          pipeline: [
            {
              $lookup: {
                from: ProductosBD.collection.name, //Nombre de la colecccion a relacionar
                localField: "productoId", //Nombre del campo de la coleccion actual
                foreignField: "_id", //Nombre del campo de la coleccion a relacionar
                as: "productoActualizado", //Nombre del campo donde se insertara todos los documentos relacionados
              },
            },
          ],
        },
      },
      {
        $lookup: {
          from: "punto_entregas",
          localField: "punto_entrega", //Nombre del campo de la coleccion actual
          foreignField: "_id", //Nombre del campo de la coleccion a relacionar
          as: "data_punto", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
    ]).exec(callback);
  },
  getPedidosPorPuntoDistFecha: async function (obj, callback) {
    var ObjectId = require("mongoose").Types.ObjectId;
    const query = {
      punto_entrega: new ObjectId(obj.punto_entrega),
      //distribuidor:  new ObjectId(obj.distribuidores)
    };
    if (obj.distribuidores) {
      query["distribuidor"] = [];
      for (const aux of obj.distribuidores) {
        query.distribuidor.push(new ObjectId(aux));
      }
    }
    const distributors = await this.aggregate([
      { $match: query },
      {
        $lookup: {
          from: pedidoTrackingBD.collection.name,
          localField: "_id",
          foreignField: "pedido",
          pipeline: [
            {
              $sort: { createdAt: -1 },
            },
          ],
          as: "tracking",
        },
      },
    ]);

    this.populate(distributors, [
      { path: "punto_entrega" },
      { path: "distribuidor" },
      { path: "codigo_descuento" },
      { path: "trabajador" },
      {
        path: "productos",
        populate: {
          path: "product",
          populate: {
            path: "categoria_producto marca_producto linea_producto",
          },
        },
      },
    ])
      .then((result) => {
        callback(null, result);
      })
      .catch((e) => {
        callback(e, null);
      });
  },
  getGraficasMarcasMeses: function (query, callback) {
    this.aggregate([
      {
        $match: {
          monedaDestino: "VES",
          estado: "TX Pagada",
          createdAt: {
            $gte: new Date(query.inicio),
            $lte: new Date(query.fin),
          },
        },
      },
      {
        $lookup: {
          from: ProductosBD.collection.name, //Nombre de la colecccion a relacionar
          localField: "productos.product", //Nombre del campo de la coleccion actual
          foreignField: "_id", //Nombre del campo de la coleccion a relacionar
          pipeline: [
            {
              $lookup: {
                from: MarcaBD.collection.name, //Nombre de la colecccion a relacionar
                localField: "marca_producto", //Nombre del campo de la coleccion actual
                foreignField: "_id", //Nombre del campo de la coleccion a relacionar
                as: "marca_productos_query", //Nombre del campo donde se insertara todos los documentos relacionados
              },
            },
          ],
          as: "data_productos_compra", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
      {
        $unwind: "$data_productos_compra",
      },
      {
        $replaceRoot: {
          newRoot: "$data_productos_compra",
        },
      },
      {
        $unwind: "$marca_productos_query",
      },
      {
        $replaceRoot: {
          newRoot: "$marca_productos_query",
        },
      },
      {
        $group: {
          _id: "$nombre",
          cantidad: { $sum: 1 },
        },
      },
      {
        $sort: { cantidad: -1 },
      },
    ]).exec(callback);
  },
  productos_comprados_user: function (id, callback) {
    const query_estado_pedidos = {
      $or: [
        { estado: "Entregado" },
        { estado: "Recibido" },
        { estado: "Calificado" },
      ],
    };
    var ObjectId = require("mongoose").Types.ObjectId;
    this.aggregate([
      {
        $match: { usuario_horeca: new ObjectId(id) },
      },
      {
        $match: query_estado_pedidos,
      },
      {
        $lookup: {
          from: ProductosBD.collection.name, //Nombre de la colecccion a relacionar
          localField: "productos.product", //Nombre del campo de la coleccion actual
          foreignField: "_id", //Nombre del campo de la coleccion a relacionar
          pipeline: [
            {
              $lookup: {
                from: LineaBD.collection.name, //Nombre de la colecccion a relacionar
                localField: "linea_producto", //Nombre del campo de la coleccion actual
                foreignField: "_id", //Nombre del campo de la coleccion a relacionar
                as: "linea_productos_query", //Nombre del campo donde se insertara todos los documentos relacionados
              },
            },
            {
              $lookup: {
                from: MarcaBD.collection.name, //Nombre de la colecccion a relacionar
                localField: "marca_producto", //Nombre del campo de la coleccion actual
                foreignField: "_id", //Nombre del campo de la coleccion a relacionar
                as: "marca_productos_query", //Nombre del campo donde se insertara todos los documentos relacionados
              },
            },
            {
              $lookup: {
                from: CategoriaBD.collection.name, //Nombre de la colecccion a relacionar
                localField: "categoria_producto", //Nombre del campo de la coleccion actual
                foreignField: "_id", //Nombre del campo de la coleccion a relacionar
                as: "categoria_productos_query", //Nombre del campo donde se insertara todos los documentos relacionados
              },
            },
          ],
          as: "data_productos_compra", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
      {
        $unwind: "$data_productos_compra",
      },
      {
        $replaceRoot: {
          newRoot: "$data_productos_compra",
        },
      },
      {
        $group: {
          _id: "$_id",
          data_compra: {
            $push: {
              imagen: "$fotos",
              categoria: "$categoria_productos_query.nombre",
              codigo_ft: "$codigo_ft",
              nombre: "$nombre",
              marca_productos_query: "$marca_productos_query.nombre",
              cat_productos_query: "$categoria_productos_query.nombre",
              linea_productos_query: "$linea_productos_query.nombre",
              total_precio_promedio_unidad: { $sum: "$precios.precio_unidad" },
              total_precio_promedio_caja: { $sum: "$precios.precio_caja" },
              cod_feat: "$codigo_ft",
              ID: "$_id",
            },
          },
        },
      },
    ]).exec(callback);
  },
  /**
   * Recupera el historial completo de mensajes de un trabjador de un punto
   */
  getChatsActivosByPunto: function (req, callback) {
    var ObjectId = require("mongoose").Types.ObjectId;
    this.aggregate([
      {
        $match: { punto_entrega: new ObjectId(req) },
      },
      {
        $lookup: {
          from: horecaBD.collection.name, //Nombre de la colecccion a relacionar
          localField: "usuario_horeca", //Nombre del campo de la coleccion actual
          foreignField: "_id", //Nombre del campo de la coleccion a relacionar
          as: "data__horeca", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
      {
        $lookup: {
          from: MensajeDB.collection.name, //Nombre de la colecccion a relacionar
          localField: "_id", //Nombre del campo de la coleccion actual
          foreignField: "pedido", //Nombre del campo de la coleccion a relacionar
          as: "data_mensajes", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
      {
        $lookup: {
          from: "distribuidors", //Nombre de la colecccion a relacionar
          localField: "distribuidor", //Nombre del campo de la coleccion actual
          foreignField: "_id", //Nombre del campo de la coleccion a relacionar
          as: "data_distribuidor", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
      {
        $lookup: {
          from: trabajadoresBD.collection.name, //Nombre de la colecccion a relacionar
          localField: "data_mensajes.conversacion.usuario", //Nombre del campo de la coleccion actual
          foreignField: "_id", //Nombre del campo de la coleccion a relacionar
          as: "data_trabajador", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
      {
        $lookup: {
          from: "punto_entregas", //Nombre de la colecccion a relacionar
          localField: "punto_entrega", //Nombre del campo de la coleccion actual
          foreignField: "_id", //Nombre del campo de la coleccion a relacionar
          as: "data_punto", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
      {
        $lookup: {
          from: "pedido_trackings", //Nombre de la colecccion a relacionar
          localField: "_id", //Nombre del campo de la coleccion actual
          foreignField: "pedido", //Nombre del campo de la coleccion a relacionar
          pipeline: [
            {
              $sort: { createdAt: -1 },
            },
            {
              $project: {
                estado_nuevo: "$estado_nuevo",
              },
            },
          ],
          as: "data_tracking", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
      {
        $lookup: {
          from: MensajeDB.collection.name, //Nombre de la colecccion a relacionar
          localField: "_id", //Nombre del campo de la coleccion actual
          foreignField: "pedido", //Nombre del campo de la coleccion a relacionar
          pipeline: [
            {
              $sort: { createdAt: -1 },
            },
            {
              $project: {
                ultimo_mensaje: { $arrayElemAt: ["$conversacion", 0] },
              },
            },
            {
              $project: {
                ultimo_mensaje_leido: "$ultimo_mensaje.leido",
                ultimo_mensaje_tiempo: "$ultimo_mensaje._id",
              },
            },
          ],
          as: "data_ultimo_mensaje", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
      {
        $sort: { distribuidor: -1 },
      },
      {
        $sort: { "data_ultimo_mensaje.ultimo_mensaje_tiempo": -1 },
      },
    ]).exec(callback);
  },
  /**
   * Recupera el historial completo de mensajes de un trabjador de distribuidor
   */
  getChatsActivosByDistribuidor: function (req, callback) {
    var ObjectId = require("mongoose").Types.ObjectId;
    this.aggregate([
      {
        $match: { distribuidor: new ObjectId(req) },
      },
      {
        $lookup: {
          from: horecaBD.collection.name, //Nombre de la colecccion a relacionar
          localField: "usuario_horeca", //Nombre del campo de la coleccion actual
          foreignField: "_id", //Nombre del campo de la coleccion a relacionar
          as: "data__horeca", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
      {
        $lookup: {
          from: MensajeDB.collection.name, //Nombre de la colecccion a relacionar
          localField: "_id", //Nombre del campo de la coleccion actual
          foreignField: "pedido", //Nombre del campo de la coleccion a relacionar
          as: "data_mensajes", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
      {
        $lookup: {
          from: "distribuidors", //Nombre de la colecccion a relacionar
          localField: "distribuidor", //Nombre del campo de la coleccion actual
          foreignField: "_id", //Nombre del campo de la coleccion a relacionar
          as: "data_distribuidor", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
      {
        $lookup: {
          from: trabajadoresBD.collection.name, //Nombre de la colecccion a relacionar
          localField: "data_mensajes.conversacion.usuario", //Nombre del campo de la coleccion actual
          foreignField: "_id", //Nombre del campo de la coleccion a relacionar
          as: "data_trabajador", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
      {
        $lookup: {
          from: "punto_entregas", //Nombre de la colecccion a relacionar
          localField: "punto_entrega", //Nombre del campo de la coleccion actual
          foreignField: "_id", //Nombre del campo de la coleccion a relacionar
          as: "data_punto", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
      {
        $lookup: {
          from: MensajeDB.collection.name, //Nombre de la colecccion a relacionar
          localField: "_id", //Nombre del campo de la coleccion actual
          foreignField: "pedido", //Nombre del campo de la coleccion a relacionar
          pipeline: [
            {
              $sort: { createdAt: -1 },
            },
            {
              $project: {
                ultimo_mensaje: { $arrayElemAt: ["$conversacion", 0] },
              },
            },
            {
              $project: {
                ultimo_mensaje_leido: "$ultimo_mensaje.leido",
                ultimo_mensaje_tiempo: "$ultimo_mensaje._id",
              },
            },
          ],
          as: "data_ultimo_mensaje", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
      {
        $sort: { distribuidor: -1 },
      },
      {
        $sort: { "data_ultimo_mensaje.ultimo_mensaje_tiempo": -1 },
      },
    ]).exec(callback);
  },
  /**
   * Recupera el historial completo de pedidosde distribuidor
   */
  AllPedidosPorDistribuidor: function (req, callback) {
    var ObjectId = require("mongoose").Types.ObjectId;
    this.aggregate([

      {
        $match: { distribuidor: new ObjectId(req) },
      },
      {
        $lookup: {
          from: "distribuidors", //Nombre de la colecccion a relacionar
          localField: "distribuidor", //Nombre del campo de la coleccion actual
          foreignField: "_id", //Nombre del campo de la coleccion a relacionar
          as: "data_distribuidor", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
      {
        $lookup: {
          from: horecaBD.collection.name, //Nombre de la colecccion a relacionar
          localField: "usuario_horeca", //Nombre del campo de la coleccion actual
          foreignField: "_id", //Nombre del campo de la coleccion a relacionar
          as: "data_horeca", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
      {
        $lookup: {
          from: "punto_entregas",
          localField: "punto_entrega", //Nombre del campo de la coleccion actual
          foreignField: "_id", //Nombre del campo de la coleccion a relacionar
          as: "data_punto", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
      {
        $lookup: {
          from: "distribuidores_vinculados",
          let: {
            punto: "$punto_entrega",
            distribuidor: "$distribuidor",
          },
          pipeline: [
            {
              $match: {
                $expr: {
                  $and: [
                    {
                      $eq: ["$punto_entrega", "$$punto"],
                    },
                    {
                      $eq: ["$distribuidor", "$$distribuidor"],
                    },
                  ],
                },
              },
            },
            {
              $project: {
                vendedor: "$vendedor",
              },
            },
            {
              $lookup: {
                from: trabajadoresBD.collection.name, //Nombre de la colecccion a relacionar
                localField: "vendedor", //Nombre del campo de la coleccion actual
                foreignField: "_id", //Nombre del campo de la coleccion a relacionar
                pipeline: [
                  {
                    $project: {
                      distribuidor: "$distribuidor",
                      nombres: "$nombres",
                      apellidos: "$apellidos",
                    },
                  },
                ],
                as: "data_trabajador", //Nombre del campo donde se insertara todos los documentos relacionados
              },
            },
          ],
          as: "data_vinculacion", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
      {
        $project: {
          _id: "$_id",
          idPedido: "$id_pedido",
          estado: "$estado",
          calificacion: "$calificacion",
          valor_pedido: "$total_pedido",
          establecimiento: "$data_horeca.nombre_establecimiento",
          punto_entrega: "$data_punto.nombre",
          nit: "$data_horeca.nit",
          pais: "$data_horeca.pais",
          departamento: "$data_punto.departamento",
          ciudad: "$data_punto.ciudad",
          metodo_pago: "$metodo_pago" || "",
          tipo_negocio: "$data_horeca.tipo_negocio",
          tipo_usuario: "$data_horeca.tipo_usuario",
          fecha: "$fecha",
          tiempo_tracking_hora: "$tiempo_tracking_hora",
          numero_productos: { $size: "$productos" },
          puntos: "$puntos_ganados",
          puntos_redimidos: "$puntos_redimidos",
          codigo_descuento: "$codigo_descuento",
          data_trabajador: "$data_trabajador",
          equipo_comercial: "$data_vinculacion.data_trabajador",
          puntoId: "$puntoId",
        },
      },
      {
        $sort: { fecha: -1 },
      },
    ]).exec(callback);
  },
  /**
   * Recupera el historial completo de pedidos sugeridos de distribuidor
   */
  AllPedidosSugeridosPorDistribuidor: function (req, callback) {
    var ObjectId = require("mongoose").Types.ObjectId;
    this.aggregate([
      {
        $match: { distribuidor: new ObjectId(req) },
      },
      {
        $lookup: {
          from: "distribuidors", //Nombre de la colecccion a relacionar
          localField: "distribuidor", //Nombre del campo de la coleccion actual
          foreignField: "_id", //Nombre del campo de la coleccion a relacionar
          as: "data_distribuidor", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
      {
        $lookup: {
          from: horecaBD.collection.name, //Nombre de la colecccion a relacionar
          localField: "usuario_horeca", //Nombre del campo de la coleccion actual
          foreignField: "_id", //Nombre del campo de la coleccion a relacionar
          as: "data_horeca", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
      {
        $lookup: {
          from: "punto_entregas",
          localField: "punto_entrega", //Nombre del campo de la coleccion actual
          foreignField: "_id", //Nombre del campo de la coleccion a relacionar
          as: "data_punto", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
      {
        $lookup: {
          from: "distribuidores_vinculados",
          // localField: "distribuidor", //Nombre del campo de la coleccion actual
          // foreignField: "distribuidor", //Nombre del campo de la coleccion a relacionar.
          localField: "punto_entrega", //Nombre del campo de la coleccion actual
          foreignField: "punto_entrega", //Nombre del campo de la coleccion a relacionar
          pipeline: [
            {
              $project: {
                vendedor: "$vendedor",
              },
            },
            {
              $lookup: {
                from: trabajadoresBD.collection.name, //Nombre de la colecccion a relacionar
                localField: "vendedor", //Nombre del campo de la coleccion actual
                foreignField: "_id", //Nombre del campo de la coleccion a relacionar
                pipeline: [
                  {
                    $project: {
                      distribuidor: "$distribuidor",
                      nombres: "$nombres",
                      apellidos: "$apellidos",
                    },
                  },
                ],
                as: "data_trabajador", //Nombre del campo donde se insertara todos los documentos relacionados
              },
            },
          ],
          as: "data_vinculacion", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
      {
        $lookup: {
          from: "pedido_trackings",
          localField: "_id", //Nombre del campo de la coleccion actual
          foreignField: "pedido", //Nombre del campo de la coleccion a relacionar
          pipeline: [
            {
              $match: { estado_anterior: "Sugerido" },
            },
          ],
          as: "data_tracking_pedido_sugerido", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
      {
        $project: {
          _id: "$_id",
          idPedido: "$id_pedido",
          estado: "$estado",
          calificacion: "$calificacion",
          valor_pedido: "$total_pedido",
          establecimiento: "$data_horeca.nombre_establecimiento",
          punto_entrega: "$data_punto.nombre",
          nit: "$data_horeca.nit",
          pais: "$data_horeca.pais",
          departamento: "$data_horeca.departamento",
          ciudad: "$data_horeca.ciudad",
          tipo_negocio: "$data_horeca.tipo_negocio",
          tipo_usuario: "$data_horeca.tipo_usuario",
          fecha: "$fecha",
          tiempo_tracking_hora: "$tiempo_tracking_hora",
          numero_productos: { $size: "$productos" },
          puntos: "$puntos_ganados",
          puntos_redimidos: "$puntos_redimidos",
          codigo_descuento: "$codigo_descuento",
          data_trabajador: "$data_trabajador",
          equipo_comercial: "$data_vinculacion.data_trabajador",
          puntoId: "$puntoId",
          data_tracking_pedido_sugerido: "$data_tracking_pedido_sugerido",
        },
      },
      {
        $sort: { fecha: -1 },
      },
    ]).exec(callback);
  },
  /**
   * Recupera el historial completo de mensajes de un trabjador de distribuidor
   */
  AllPedidosPorHoreca: function (req, callback) {
    var ObjectId = require("mongoose").Types.ObjectId;
    this.aggregate([
      {
        $match: { usuario_horeca: new ObjectId(req.usuario_horeca) },
      },
      {
        $sort: { createdAt: -1 },
      },
      {
        $lookup: {
          from: "distribuidors", //Nombre de la colecccion a relacionar
          localField: "distribuidor", //Nombre del campo de la coleccion actual
          foreignField: "_id", //Nombre del campo de la coleccion a relacionar
          pipeline: [
            {
              $project: {
                distribuidor: "$distribuidor",
                nombre: "$nombre",
                logo: "$logo",
              },
            },
          ],
          as: "data_distribuidor", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
      {
        $project: {
          _id: "$_id",
          total_pedido: "$total_pedido",
          id_pedido: "$id_pedido",
          estado: "$estado",
          createdAt: "$createdAt",
          nombre: "$nombre",
          fecha: "$fecha",
          producto: "$producto",
          usuario_horeca: "$usuario_horeca",
          trabajador: "$trabajador",
          productos: "$productos",
          data_distribuidor: "$data_distribuidor",
          punto_entrega: "$punto_entrega",
        },
      },
    ]).exec(callback);
  },

  /**
   * Información para construir la prefactura
   */
  getPrefactura: function (req, callback) {
    var ObjectId = require("mongoose").Types.ObjectId;
    this.aggregate([
      {
        $match: { _id: new ObjectId(req._id) },
      },
      {
        $lookup: {
          from: ProductosBD.collection.name,
          localField: "productos.product", //Nombre del campo de la coleccion actual
          foreignField: "_id", //Nombre del campo de la coleccion a relacionar
          pipeline: [
            {
              $project: {
                nombre: "$nombre",
                marca_producto: "$marca_producto",
                codigo_distribuidor_producto: "$codigo_distribuidor_producto",
                precios: "$precios",
                saldos: "$saldos",
                promocion: "$promocion",
                codigo_promo: "$codigo_promo",
              },
            },
            {
              $lookup: {
                from: MarcaBD.collection.name, //Nombre de la colecccion a relacionar
                localField: "marca_producto", //Nombre del campo de la coleccion actual
                foreignField: "_id", //Nombre del campo de la coleccion a relacionar
                pipeline: [
                  {
                    $project: {
                      nombre: "$nombre",
                    },
                  },
                ],
                as: "nombre_marca", //Nombre del campo donde se insertara todos los documentos relacionados
              },
            },
          ],
          as: "data_producto", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
      {
        $lookup: {
          from: "distribuidors", //Nombre de la colecccion a relacionar
          localField: "distribuidor", //Nombre del campo de la coleccion actual
          foreignField: "_id", //Nombre del campo de la coleccion a relacionar
          pipeline: [
            {
              $project: {
                distribuidor: "$distribuidor",
                nombre: "$nombre",
                logo: "$logo",
              },
            },
          ],
          as: "data_distribuidor", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
      {
        $lookup: {
          from: horecaBD.collection.name, //Nombre de la colecccion a relacionar
          localField: "usuario_horeca", //Nombre del campo de la coleccion actual
          foreignField: "_id", //Nombre del campo de la coleccion a relacionar
          pipeline: [
            {
              $project: {
                nombre_establecimiento: "$nombre_establecimiento",
                nit: "$nit",

              },
            },
          ],
          as: "data_horeca", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
      {
        $lookup: {
          from: "punto_entregas",
          localField: "punto_entrega", //Nombre del campo de la coleccion actual
          foreignField: "_id", //Nombre del campo de la coleccion a relacionar
          pipeline: [
            {
              $project: {
                nombre_punto: "$nombre",
                departamento: "$departamento",
                direccion: "$direccion"

              },
            },
          ],
          as: "data_punto", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
      {
        $lookup: {
          from: "codigos_descuento_generados",
          localField: "codigo_descuento", //Nombre del campo de la coleccion actual
          foreignField: "_id", //Nombre del campo de la coleccion a relacionar
          as: "data_cod_desc", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
      {
        $project: {
          _id: "$_id",
          total_pedido: "$total_pedido",
          id_pedido: "$id_pedido",
          estado: "$estado",
          puntos_ganados: "$puntos_ganados",
          puntos_redimidos: "$puntos_redimidos",
          productos: "$productos",
          nombre_distribuidor: "$data_distribuidor.nombre",
          nombre_horeca: "$data_horeca.nombre_establecimiento",
          nit_horeca: "$data_horeca.nit",
          data_punto: "$data_punto",
          nombre_punto: "$data_punto.nombre_punto",
          data_producto: "$data_producto",
          codigos_list: "$data_cod_desc",
        },
      },
    ]).exec(callback);
  },
  /**
   * Recupera la calificación/ranking por trabajador
   */
  calificacionPorDistribuidor: function (req, callback) {
    var ObjectId = require("mongoose").Types.ObjectId;
    this.aggregate([
      {
        $match: { distribuidor: new ObjectId(req) },
      },
      {
        $project: {
          _id: "$_id",
          distribuidor: "$distribuidor",
          calificacion: "$calificacion",
        },
      },
    ]).exec(callback);
  },
  /**
   * Recupera la calificación/ranking por trabajador
   */
  getTotalCalificacionesDistribuidor: function (req, callback) {
    var ObjectId = require("mongoose").Types.ObjectId;
    this.aggregate([
      {
        $match: { distribuidor: new ObjectId(req) },
      },
      {
        $unwind: "$calificacion",
      },
      {
        $replaceRoot: {
          newRoot: "$calificacion",
        },
      },
    ]).exec(callback);
  },
  /**
   * Lista de ventas por organización
   */
  getVentasPorOrganizacion: function (query, callback) {
    var ObjectId = require("mongoose").Types.ObjectId;
    this.find(query).exec(callback);
  },
  /** Pedido de un producto en los ultimos 3meses */
  getTotalPedidosProductosTresMeses: function (obj, callback) {
    const d = new Date();
    const fechaReferencia = d.setMonth(d.getMonth() - 3);
    const ObjectId = require("mongoose").Types.ObjectId;
    const query = {
      "productos.product": new ObjectId(obj),
      fecha: {
        $gte: new Date(fechaReferencia),
      },
    };
    Pedido.count(query, callback);
  },
  /** Establecimeintos que pidieron el producto en los ultimos 3meses */
  countPedidoProductoEstablecimiento: function (obj, callback) {
    const d = new Date();
    const fechaReferencia = d.setMonth(d.getMonth() - 3);
    const ObjectId = require("mongoose").Types.ObjectId;
    const query = {
      "productos.product": new ObjectId(obj),
      fecha: {
        $gte: new Date(fechaReferencia),
      },
    };
    Pedido.find(query, callback);
  },
  /**
   * Recupera el historial completo de mensajes de un trabjador de un punto
   */
  getMensajesSinLeerEstablecimiento: function (req, callback) {
    const query_estado_pedidos = {
      $or: [
        { estado: "Pendiente" },
        { estado: "Aprobado Interno" },
        { estado: "Aprobado Externo" },
        { estado: "Alistamiento" },
        { estado: "Despachado" },
        { estado: "Facturado" },
      ],
    };
    var ObjectId = require("mongoose").Types.ObjectId;
    this.aggregate([
      {
        $match: { usuario_horeca: new ObjectId(req.idHoreca) },
      },
      {
        $match: query_estado_pedidos,
      },
      {
        $lookup: {
          from: MensajeDB.collection.name, //Nombre de la colecccion a relacionar
          localField: "_id", //Nombre del campo de la coleccion actual
          foreignField: "pedido", //Nombre del campo de la coleccion a relacionar
          pipeline: [
            {
              $match: {
                estado: "Activo",
              },
            },
            {
              $project: {
                pedido_id: "$_id",
                pedido_conversacion: "$conversacion",
              },
            },
          ],
          as: "data_mensajes", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
      {
        $project: {
          pedido_id: { $arrayElemAt: ["$data_mensajes.pedido_id", 0] },
          pedido_conversacion: {
            $arrayElemAt: ["$data_mensajes.pedido_conversacion", 0],
          },
        },
      },
      {
        $project: {
          pedido_id: "$pedido_id",
          pedido_conversacion: {
            $arrayElemAt: ["$pedido_conversacion", -1],
          },
        },
      },
      {
        $project: {
          pedido_id: "$pedido_id",
          pedido_leido: "$pedido_conversacion.leido",
        },
      },
      {
        $group: {
          _id: "$pedido_leido",
          total: { $sum: 1 },
        },
      },
      {
        $match: {
          _id: false,
        },
      },
    ]).exec(callback);
  },
  /**
   * Recupera el total de pedidos sugeridos de un establecimiento
   */
  getPedidosSugeridosNew: function (req, callback) {
    var ObjectId = require("mongoose").Types.ObjectId;
    this.aggregate([
      {
        $match: {
          usuario_horeca: new ObjectId(req.idHoreca),
          estado: "Sugerido",
        },
      },
      {
        $group: {
          _id: "$estado",
          total: { $sum: 1 },
        },
      },
    ]).exec(callback);
  },
  getPedidosSugeridos: function (obj, callback) {
    var ObjectId = require("mongoose").Types.ObjectId;
    const query = {
      estado: "Sugerido",
      trabajador: new ObjectId(obj.trabajador),
    };
    Pedido.count(query, callback);
  },
  /**
   * Recupera el total de pedidos realizados de un establecimiento
   */
  getPedidosRealizados: function (req, callback) {
    const query_estado_pedidos = {
      $or: [
        { estado: "Pendiente" },
        { estado: "Aprobado Interno" },
        { estado: "Aprobado Externo" },
        { estado: "Alistamiento" },
        { estado: "Despachado" },
        { estado: "Facturado" },
        { estado: "Entregado" },
        { estado: "Recibido" },
        { estado: "Calificado" },
      ],
    };
    var ObjectId = require("mongoose").Types.ObjectId;
    this.aggregate([
      {
        $match: {
          usuario_horeca: new ObjectId(req.idHoreca),
        },
      },
      {
        $match: query_estado_pedidos,
      },
      {
        $group: {
          _id: "$usuario_horeca",
          total: { $sum: 1 },
        },
      },
    ]).exec(callback);
  },
  /********************************* Graficas  ********************************/
  /** Tabla con data de la cantidad de pedidos por distribuidor */
  getInformeBarraHorecaPedidosDistribuidor: function (query, callback) {
    const query_estado_pedidos = {
      $or: [
        { estado: "Entregado" },
        { estado: "Recibido" },
        { estado: "Calificado" },
      ],
    };
    var ObjectId = require("mongoose").Types.ObjectId;
    this.aggregate([
      {
        $match: {
          usuario_horeca: new ObjectId(query.idHoreca),
          fecha: {
            $gte: new Date(query.inicio),
            $lte: new Date(query.fin),
          },
        },
      },
      {
        $match: query_estado_pedidos,
      },
      {
        $lookup: {
          from: "distribuidors",
          localField: "distribuidor", //Nombre del campo de la coleccion actual
          foreignField: "_id", //Nombre del campo de la coleccion a relacionar
          pipeline: [
            {
              $project: {
                distribuidor_nombre: "$nombre",
              },
            },
          ],
          as: "data_distribuidor", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
      {
        $group: {
          _id: "$distribuidor",
          distribuidor_nombre: {
            $first: {
              $arrayElemAt: ["$data_distribuidor.distribuidor_nombre", 0],
            },
          },
          total: { $sum: 1 },
        },
      },
      {
        $sort: { total: -1 },
      },
    ]).exec(callback);
  },
  /** Tabla con data de la cantidad de pedidos por mes */
  getInformeBarraHorecaPedidosMes: function (query, callback) {
    var ObjectId = require("mongoose").Types.ObjectId;

    const query_estado_pedidos = {
      $or: [
        { estado: "Entregado" },
        { estado: "Recibido" },
        { estado: "Calificado" },
      ],
    };
    this.aggregate([
      {
        $match: {
          usuario_horeca: new ObjectId(query.idHoreca),
          fecha: {
            $gte: new Date(query.inicio),
            $lte: new Date(query.fin),
          },
        },
      },
      {
        $match: query_estado_pedidos,
      },
      /** agrega la fecha del pedido */
      {
        $addFields: {
          producto_fecha_pedido: {
            $dateToString: {
              format: "%Y-%m",
              date: "$fecha",
            },
          },
        },
      },
      {
        $group: {
          _id: "$producto_fecha_pedido",
          total: { $sum: 1 },
        },
      },
      {
        $sort: { _id: 1 },
      },
    ]).exec(callback);
  },
  /** Tabla con data de la cantidad de pedidos por comprados por distribuudor por punto */
  getInformePieDistribuidoresCompras: function (query, callback) {
    var ObjectId = require("mongoose").Types.ObjectId;
    this.aggregate([
      {
        $match: {
          punto_entrega: new ObjectId(query.idPunto),
        },
      },
      {
        $lookup: {
          from: "distribuidors",
          localField: "distribuidor", //Nombre del campo de la coleccion actual
          foreignField: "_id", //Nombre del campo de la coleccion a relacionar
          pipeline: [
            {
              $project: {
                distribuidor_nombre: "$nombre",
              },
            },
          ],
          as: "data_distribuidor", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
      {
        $group: {
          _id: "$distribuidor",
          distribuidor_nombre: {
            $first: {
              $arrayElemAt: ["$data_distribuidor.distribuidor_nombre", 0],
            },
          },
          total: { $sum: "$total_pedido" },
        },
      },
      {
        $sort: { total: -1 },
      },
    ]).exec(callback);
  },
  /** Tabla con data de pedidos por distribuidor */
  getInformePieDistribuidoresPedidos: function (query, callback) {
    var ObjectId = require("mongoose").Types.ObjectId;
    this.aggregate([
      {
        $match: {
          punto_entrega: new ObjectId(query.idPunto),
        },
      },
      {
        $lookup: {
          from: "distribuidors",
          localField: "distribuidor", //Nombre del campo de la coleccion actual
          foreignField: "_id", //Nombre del campo de la coleccion a relacionar
          pipeline: [
            {
              $project: {
                distribuidor_nombre: "$nombre",
              },
            },
          ],
          as: "data_distribuidor", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
      {
        $group: {
          _id: "$distribuidor",
          distribuidor_nombre: {
            $first: {
              $arrayElemAt: ["$data_distribuidor.distribuidor_nombre", 0],
            },
          },
          total: { $sum: 1 },
        },
      },
      {
        $sort: { total: -1 },
      },
    ]).exec(callback);
  },
  /** Pedidos entregados por mes de un distribuidor */
  getInformeDistribuidorPedidosEntregadosMes: function (query, callback) {
    let ObjectId = require("mongoose").Types.ObjectId;
    this.aggregate([
      {
        $match: {
          distribuidor: new ObjectId(query.idDistribuidor),
          fecha: {
            $gte: new Date(query.inicio),
            $lte: new Date(query.fin),
          },
          $or: [
            { estado: "Entregado" },
            { estado: "Recibido" },
            { estado: "Calificado" },
          ],
        },
      },
      /** agrega la fecha del pedido */
      {
        $addFields: {
          producto_fecha_pedido: {
            $dateToString: {
              format: "%Y-%m",
              date: "$fecha",
            },
          },
        },
      },
      {
        $group: {
          _id: "$producto_fecha_pedido",
          total: { $sum: 1 },
        },
      },
      {
        $sort: { _id: 1 },
      },
    ]).exec(callback);
  },
  /** Pedidos entregados por mes de un distribuidor */
  getInformeDistribuidorPedidosEntregadosAnio: function (query, callback) {
    let ObjectId = require("mongoose").Types.ObjectId;
    this.aggregate([
      {
        $match: {
          distribuidor: new ObjectId(query.idDistribuidor),
          fecha: {
            $gte: new Date(query.inicio),
            $lte: new Date(query.fin),
          },
          $or: [
            { estado: "Entregado" },
            { estado: "Recibido" },
            { estado: "Calificado" },
          ],
        },
      },
      /** agrega la fecha del pedido */
      {
        $addFields: {
          producto_fecha_pedido: {
            $dateToString: {
              format: "%Y-%m",
              date: "$fecha",
            },
          },
        },
      },
      {
        $group: {
          _id: "$producto_fecha_pedido",
          total: { $sum: 1 },
        },
      },
      {
        $sort: { _id: 1 },
      },
    ]).exec(callback);
  },
  /** Pedidos estados actuales */
  getInformeDistribuidorPedidosEstados: function (query, callback) {
    let ObjectId = require("mongoose").Types.ObjectId;
    this.aggregate([
      {
        $match: {
          distribuidor: new ObjectId(query.idDistribuidor),
        },
      },
      {
        $group: {
          _id: "$estado",
          total: { $sum: 1 },
        },
      },
      {
        $sort: { total: 1 },
      },
    ]).exec(callback);
  },
  /** Pedidos estados actuales por filtro mes-añoo */
  getInformeDistribuidorPedidosEstadosPorMes: function (query, callback) {
    let ObjectId = require("mongoose").Types.ObjectId;
    this.aggregate([
      {
        $match: {
          distribuidor: new ObjectId(query.idDistribuidor),
          fecha: {
            $gte: new Date(query.inicio),
            $lte: new Date(query.fin),
          },
        },
      },
      {
        $group: {
          _id: "$estado",
          total: { $sum: 1 },
        },
      },
      {
        $project: { total: -1 },
      },
    ]).exec(callback);
  },
  /** Pedidos por tipo de ngeocio */
  getInformeDistribuidorPedidosTipoNegocio: function (query, callback) {
    let ObjectId = require("mongoose").Types.ObjectId;
    this.aggregate([
      {
        $match: {
          distribuidor: new ObjectId(query.idDistribuidor),
          fecha: {
            $gte: new Date(query.inicio),
            $lte: new Date(query.fin),
          },
          $or: [
            { estado: "Entregado" },
            { estado: "Recibido" },
            { estado: "Calificado" },
          ],
        },
      },
      {
        $project: {
          usuario_horeca: "$usuario_horeca",
        },
      },
      {
        $lookup: {
          from: "usuario_horecas",
          localField:
            "usuario_horeca" /** Nombre del campo de la coleccion actual */,
          foreignField:
            "_id" /** Nombre del campo de la coleccion a relacionar */,
          pipeline: [
            {
              $project: {
                tipo_negocio: "$tipo_negocio",
              },
            },
          ],
          as: "data_horeca" /** Nombre del campo donde se insertara todos los documentos relacionados */,
        },
      },
      {
        $unwind: "$data_horeca",
      },
      {
        $replaceRoot: {
          newRoot: "$data_horeca",
        },
      },
      {
        $group: {
          _id: "$tipo_negocio",
          total: { $sum: 1 },
        },
      },
      {
        $sort: { total: -1 },
      },
    ]).exec(callback);
  },
  getInformeDistribuidorPedidosCiudad: function (query, callback) {
    let ObjectId = require("mongoose").Types.ObjectId;
    this.aggregate([
      {
        $match: {
          distribuidor: new ObjectId(query.idDistribuidor),
          fecha: {
            $gte: new Date(query.inicio),
            $lte: new Date(query.fin),
          },
          $or: [
            { estado: "Entregado" },
            { estado: "Recibido" },
            { estado: "Calificado" },
          ],
        },
      },
      {
        $group: {
          _id: "$ciudad",
          total: { $sum: 1 },
        },
      },
      {
        $sort: { total: -1 },
      },
    ]).exec(callback);
  },
  /** Pedidos  por mes de un distribuidor */
  getInformeDistribuidorPedidosMes: function (query, callback) {
    let ObjectId = require("mongoose").Types.ObjectId;
    this.aggregate([
      {
        $match: {
          distribuidor: new ObjectId(query.idDistribuidor),
          fecha: {
            $gte: new Date(query.inicio),
            $lte: new Date(query.fin),
          },
          $or: [
            { estado: "Entregado" },
            { estado: "Recibido" },
            { estado: "Calificado" },
          ],
        },
      },
      /** agrega la fecha del pedido */
      {
        $addFields: {
          producto_fecha_pedido: {
            $dateToString: {
              format: "%Y-%m",
              date: "$fecha",
            },
          },
        },
      },
      {
        $group: {
          _id: "$producto_fecha_pedido",
          total: { $sum: 1 },
        },
      },
      {
        $sort: { _id: 1 },
      },
    ]).exec(callback);
  },
  /** establecimientos  por mes de un distribuidor */
  establecimientos_alcanzados_mes: function (query, callback) {
    let ObjectId = require("mongoose").Types.ObjectId;
    this.aggregate([
      {
        $match: {
          distribuidor: new ObjectId(query.idDistribuidor),
          fecha: {
            $gte: new Date(query.inicio),
            $lte: new Date(query.fin),
          },
          $or: [
            { estado: "Entregado" },
            { estado: "Recibido" },
            { estado: "Calificado" },
          ],
        },
      },
      /** agrega la fecha del pedido */
      {
        $addFields: {
          producto_fecha_pedido: {
            $dateToString: {
              format: "%Y-%m",
              date: "$fecha",
            },
          },
        },
      },
      {
        $group: {
          _id: {
            punto_entrega: "$punto_entrega",
            producto_fecha_pedido: "$producto_fecha_pedido",
          },
        },
      },
      {
        $group: {
          _id: "$_id.producto_fecha_pedido",
          total: { $sum: 1 },
        },
      },
      {
        $sort: { _id: 1 },
      },
    ]).exec(callback);
  },
  /** TABLA pedidos distribuidor */
  getInformeDistribuidorTablaPedidos: function (req, callback) {
    var ObjectId = require("mongoose").Types.ObjectId;
    this.aggregate([
      {
        $match: { distribuidor: new ObjectId(req.idDistribuidor) },
      },
      {
        $lookup: {
          from: horecaBD.collection.name, //Nombre de la colecccion a relacionar
          localField: "usuario_horeca", //Nombre del campo de la coleccion actual
          foreignField: "_id", //Nombre del campo de la coleccion a relacionar
          as: "data_horeca", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
      {
        $lookup: {
          from: "punto_entregas",
          localField: "punto_entrega", //Nombre del campo de la coleccion actual
          foreignField: "_id", //Nombre del campo de la coleccion a relacionar
          as: "data_punto", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
      {
        $lookup: {
          from: "codigos_descuento_generados",
          localField: "_id", //Nombre del campo de la coleccion actual
          foreignField: "pedidos.pedido", //Nombre del campo de la coleccion a relacionar
          as: "data_cod_descuento", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
      {
        $lookup: {
          from: "distribuidores_vinculados",
          localField: "punto_entrega", //Nombre del campo de la coleccion actual
          foreignField: "punto_entrega", //Nombre del campo de la coleccion a relacionar
          pipeline: [
            {
              $match: { distribuidor: new ObjectId(req.idDistribuidor) },
            },
            {
              $lookup: {
                from: trabajadoresBD.collection.name, //Nombre de la colecccion a relacionar
                localField: "vendedor", //Nombre del campo de la coleccion actual
                foreignField: "_id", //Nombre del campo de la coleccion a relacionar
                as: "data_trabajador", //Nombre del campo donde se insertara todos los documentos relacionados
              },
            },
          ],
          as: "data_vinculacion", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
      {
        $lookup: {
          from: "pedido_trackings",
          localField: "_id", //Nombre del campo de la coleccion actual
          foreignField: "pedido", //Nombre del campo de la coleccion a relacionar
          pipeline: [
            {
              $facet: {
                fecha_aprobado: [
                  {
                    $match: { estado_nuevo: "Aprobado Externo" },
                  },
                  {
                    $project: {
                      estado_aprobado: "$estado_nuevo",
                      fecha_aprobado: "$createdAt",
                    },
                  },
                ],
                fecha_entrega: [
                  {
                    $match: { estado_nuevo: "Entregado" },
                  },
                  {
                    $project: {
                      estado_entrega: "$estado_nuevo",
                      fecha_entrega: "$createdAt",
                    },
                  },
                ],
              },
            },
          ],
          as: "data_tracking", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
      {
        $addFields: {
          pedido_fecha_aprobado: {
            $arrayElemAt: ["$data_tracking.fecha_aprobado.fecha_aprobado", 0],
          },
          pedido_fecha_entregado: {
            $arrayElemAt: ["$data_tracking.fecha_entrega.fecha_entrega", 0],
          },
        },
      },
      {
        $project: {
          calificacion: "$calificacion",
          _id: "$_id",
          horeca_nombre: {
            $arrayElemAt: ["$data_horeca.nombre_establecimiento", 0],
          },
          horeca_tipo_usuario: {
            $arrayElemAt: ["$data_horeca.tipo_usuario", 0],
          },
          horeca_nit: { $arrayElemAt: ["$data_horeca.nit", 0] },
          horeca_pais: { $arrayElemAt: ["$data_horeca.pais", 0] },
          horeca_departamento: {
            $arrayElemAt: ["$data_punto.departamento", 0],
          },
          horeca_ciudad: { $arrayElemAt: ["$data_punto.ciudad", 0] },
          horeca_tipo_negocio: {
            $arrayElemAt: ["$data_horeca.tipo_negocio", 0],
          },
          punto_nombre: { $arrayElemAt: ["$data_punto.nombre", 0] },
          pedido_fecha: "$fecha",
          pedido_id: "$id_pedido",
          pedido_estado: "$estado",
          pedido_fecha_aprobado: {
            $arrayElemAt: ["$pedido_fecha_aprobado", 0],
          },
          pedido_fecha_entregado: {
            $arrayElemAt: ["$pedido_fecha_entregado", 0],
          },
          pedido_valor: "$total_pedido",
          pedido_numero_productos: { $size: "$productos" },
          pedido_puntos: "$puntos_ganados",
          pedido_puntos_redimidos: "$puntos_redimidos",
          pedido_estado_cod_descuento: {
            $arrayElemAt: ["$data_cod_descuento.estado", 0],
          },
          pedido_cod_descuento_generado: {
            $arrayElemAt: ["$data_cod_descuento.codigo_creado", 0],
          },
          equipo_comercial: "$data_vinculacion",
        },
      },
      {
        $sort: { pedido_fecha: -1 },
      },
    ]).exec(callback);
  },
  /** Pedidos  por mes de un distribuidor */
  getTop10ProdDistMes: function (query, callback) {
    let ObjectId = require("mongoose").Types.ObjectId;
    this.aggregate([
      {
        $match: {
          distribuidor: new ObjectId(query.idDist),
          /*fecha: {
            $gte: new Date(query.inicio),
            $lte: new Date(query.fin),
          },*/
          $or: [
            { estado: "Entregado" },
            { estado: "Recibido" },
            { estado: "Calificado" },
          ],
        },
      },
      {
        $lookup: {
          from: "reportepedidos",
          localField: "productos.product", //Nombre del campo de la coleccion actual
          foreignField: "productoId", //Nombre del campo de la coleccion a relacionar
          pipeline: [
            {
              $addFields: {
                costoTotalProducto: {
                  $multiply: ["$unidadesCompradas", "$costoProductos"], // Precio final
                },
              },
            },
            {
              $project: {
                productoId: "$productoId",
                nombreProducto: "$nombreProducto",
                costoTotalProducto: "$costoTotalProducto",
                fotos: "$detalleProducto.fotos",
              },
            },
          ],
          as: "data_detalle_pedido", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
      {
        $unwind: "$data_detalle_pedido",
      },
      {
        $replaceRoot: {
          newRoot: "$data_detalle_pedido",
        },
      },
      {
        $group: {
          _id: "$productoId",
          costoTotalProducto: { $sum: "$costoTotalProducto" },
        },
      },
      {
        $sort: { costoTotalProducto: -1 },
      },
      {
        $limit: 10,
      },
      {
        $lookup: {
          from: "productos",
          localField: "_id", //Nombre del campo de la coleccion actual
          foreignField: "_id", //Nombre del campo de la coleccion a relacionar
          as: "data_productos", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
      {
        $addFields: {
          costoTotalProducto: "$data_productos.costoTotalProducto",
        },
      },
      {
        $unwind: "$data_productos",
      },
      {
        $replaceRoot: {
          newRoot: "$data_productos",
        },
      },
    ]).exec(callback);
  },
  get_traking_pedidos: function (query, callback) {
    this.aggregate([
      {
        $match: {
          createdAt: {
            $gte: new Date(query.inicio),
            $lte: new Date(query.fin),
          },
        },
      },
      {
        $group: {
          _id: "$estado",
          total_pedido: { $sum: "$total_pedido" },
          cantidad: { $sum: 1 },
        },
      },
    ]).exec(callback);
  },
  getPedidoDistAppIndicadores: function (query, callback) {
    this.aggregate([
      {
        $match: {
          distribuidor: new ObjectId(query.distribuidor),
          //Se ha comentado el filtro de mes para dejarlo procesado en totalidad hasta que se cambie la consulta en web.
          /*createdAt: {
            $gte: new Date(query.inicio),
            $lte: new Date(query.fin),
          },*/
        },
      },
      {
        $group: {
          _id: "$estado",
          total_pedido: { $sum: "$total_pedido" },
          cantidad: { $sum: 1 },
        },
      },
    ]).exec(callback);
  },
  getPedidoDistApp: function (query, callback) {
    this.aggregate([
      {
        $match: {
          _id: new ObjectId(query)
        },
      },
      {
        $lookup: {
          from: "usuario_horecas",
          localField: "usuario_horeca", //Nombre del campo de la coleccion actual
          foreignField: "_id", //Nombre del campo de la coleccion a relacionar
          as: "dataHoreca", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
      {
        $lookup: {
          from: "pedido_trackings",
          localField: "_id", //Nombre del campo de la coleccion actual
          foreignField: "pedido", //Nombre del campo de la coleccion a relacionar
          as: "traking_pedido", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
      {
        $lookup: {
          from: "distribuidors",
          localField: "distribuidor", //Nombre del campo de la coleccion actual
          foreignField: "_id", //Nombre del campo de la coleccion a relacionar
          as: "dataDistribuidor", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
      {
        $lookup: {
          from: "codigos_descuento_generados",
          localField: "codigo_descuento", //Nombre del campo de la coleccion actual
          foreignField: "_id", //Nombre del campo de la coleccion a relacionar
          as: "data_codigo_descuento", //Nombre del campo donde se insertara todos los documentos relacionados
          pipeline: [
            {
              $project: {
                codigo_creado: "$codigo_creado",
                valor_paquete: "$valor_paquete",
                valor_moneda: "$valor_moneda"
              }
            }

          ]
        },
      },
      {
        $lookup: {
          from: "pedido_trackings",
          localField: "_id", //Nombre del campo de la coleccion actual
          foreignField: "pedido", //Nombre del campo de la coleccion a relacionar
          as: "traking_pedido", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
      {
        $lookup: {
          from: "punto_entregas",
          localField: "punto_entrega", //Nombre del campo de la coleccion actual
          foreignField: "_id", //Nombre del campo de la coleccion a relacionar
          as: "dataPunto", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
      {
        $lookup: {
          from: "trabajadors",
          localField: "trabajador", //Nombre del campo de la coleccion actual
          foreignField: "_id", //Nombre del campo de la coleccion a relacionar
          as: "dataTrabajador", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
      {
        $lookup: {
          from: "reportepedidos",
          localField: "_id", //Nombre del campo de la coleccion actual
          foreignField: "idPedido", //Nombre del campo de la coleccion a relacionar
          as: "dataProductos", //Nombre del campo donde se insertara todos los documentos relacionados
          pipeline: [
            {
              $lookup: {
                from: "marca_productos", //Nombre de la colecccion a relacionar
                localField: "marca_producto", //Nombre del campo de la coleccion actual
                foreignField: "_id", //Nombre del campo de la coleccion a relacionar
                as: "data_marca", //Nombre del campo donde se insertara todos los documentos relacionados
              },
            },
            {
              $lookup: {
                from: "productos", //Nombre de la colecccion a relacionar
                localField: "productoId", //Nombre del campo de la coleccion actual
                foreignField: "_id", //Nombre del campo de la coleccion a relacionar
                as: "data_producto_original", //Nombre del campo donde se insertara todos los documentos relacionados
              },
            },
            {
              $project: {
                idProducto: "$productoId",
                nombre: "$nombreProducto",
                fotos: "$detalleProducto.fotos",
                cantidad_comprada: "$unidadesCompradas",
                total_comprado: "$costoProductos",
                precio_und: "$costoProductos",
                precio_caja: "$precio_caja_app",
                unidades_x_caja: "$detalleProducto.precios.und_x_caja",
                unidad_medida: "$detalleProducto.precios.unidad_medida",
                cantidad_medida: "$detalleProducto.precios.cantidad_medida",
                sku_distribuidor: "$detalleProducto.codigo_distribuidor_producto",
                sku: "$detalleProducto.codigo_distribuidor_producto",
                marca: "$detalleProducto.marca_producto.nombre",
                categoria: "$detalleProducto.categoria_producto.nombre",
                detalleProducto: '$detalleProducto',
                inventario: { $last: "$data_producto_original.precios.inventario_unidad" },
                puntos_ft: { $multiply: ["$unidadesCompradas", "$puntos_ft_unidad"] },
                codigo_promo: '$detalleProducto.codigo_promo',
                promocion: { $last: "$data_producto_original.promocion" },
                prodPedido: { $last: "$data_producto_original.prodPedido" },
                saldos: { $last: "$data_producto_original.saldos" },
                prodBiodegradable: { $last: "$data_producto_original.prodBiodegradable" },
                prodDescuento: { $last: "$data_producto_original.prodDescuento" },
                prodPorcentajeDesc: { $last: "$data_producto_original.prodPorcentajeDesc" },
                mostrarPF: { $last: "$data_producto_original.mostrarPF" },
                precios: { $last: "$data_producto_original.precios" },
              }
            }
          ]
        },
      },
      {
        $lookup: {
          from: "distribuidores_vinculados",
          localField: "punto_entrega", //Nombre del campo de la coleccion actual
          foreignField: "punto_entrega", //Nombre del campo de la coleccion a relacionar
          as: "dataVinculaciones", //Nombre del campo donde se insertara todos los documentos relacionados
          pipeline: [
            {
              $lookup: {
                from: "trabajadors",
                localField: "vendedor", //Nombre del campo de la coleccion actual
                foreignField: "_id", //Nombre del campo de la coleccion a relacionar
                as: "info_trabajador", //Nombre del campo donde se insertara todos los documentos relacionados
                pipeline: [
                  {
                    $project: {
                      nombre: { $concat: ["$nombres", " ", "$apellidos"] }
                    }
                  }
                ]
              },
            },
          ]
        },
      },
      {
        $project: {
          id_pedido: "$id_pedido",
          estado: "$estado",
          logo: "$dataHoreca.logo",
          nombre_punto: "$dataPunto.nombre",
          idPunto: "$dataPunto._id",
          nombre_establecimiento: "$dataHoreca.nombre_establecimiento",
          nit_establecimiento: "$dataHoreca.nit",
          id_horeca: "$dataHoreca._id",
          tipo_establecimiento: "$dataHoreca.tipo_usuario",
          tipo_negocio_establecimiento: "$dataHoreca.tipo_negocio",
          ciudad_establecimiento: "$dataHoreca.ciudad",
          departamento_establecimiento: "$dataHoreca.departamento",
          telefono_establecimiento: "$dataHoreca.telefono",
          pais: "$dataPunto.pais",
          departamento: "$dataPunto.departamento",
          correo: "$dataPunto.departamento",
          ciudad: "$ciudad",
          id_distribuidor: "$distribuidor",
          direccion: "$direccion",
          total_pedido: "$total_pedido",
          puntos_ganados: "$puntos_ganados",
          puntos_redimidos: "$puntos_redimidos",
          metodo_pago: "$metodo_pago",
          equipo_comercial: "",
          convenio: "",
          cartera: "",
          pazysalvo: "",
          mostrar_estado_proximo: true,
          email: "$dataHoreca.correo",
          productos: "$dataProductos",
          minimoPedido: "$dataDistribuidor.valor_minimo_pedido",
          dataComplete: "$dataVinculaciones",
          contacto_punto: "$dataPunto.informacion_contacto",
          data_codigo_descuento: "$data_codigo_descuento",
          fecha_actualizacion: { $last: "$traking_pedido.createdAt" },
        }
      }
    ]).exec(callback);
  },
  getPedidoDistAppIndicadoresAll: function (query, callback) {
    this.aggregate([
      {
        $match: {
          distribuidor: new ObjectId(query),
        },
      },
      {
        $group: {
          _id: "$estado",
          total_pedido: { $sum: "$total_pedido" },
          cantidad: { $sum: 1 },
        },
      },
    ]).exec(callback);
  },
  getGraficoBarsDistPedidos: function (query, callback) {
    let ObjectId = require("mongoose").Types.ObjectId;
    this.aggregate([
      {
        $match: {
          distribuidor: new ObjectId(query.distribuidor),
          fecha: {
            $gte: new Date(query.inicio),
            $lte: new Date(query.fin),
          },
          $or: [
            { estado: "Entregado" },
            { estado: "Recibido" },
            { estado: "Calificado" },
          ],
        },
      },
      /** agrega la fecha del pedido */
      {
        $addFields: {
          producto_fecha_pedido: {
            $dateToString: {
              format: "%Y-%m",
              date: "$fecha",
            },
          },
        },
      },
      {
        $group: {
          _id: "$producto_fecha_pedido",
          total: { $sum: 1 },
        },
      },
      {
        $sort: { _id: 1 },
      },
    ]).exec(callback);
  },
  pedidoUtility: function (query, callback) {
    this.aggregate([
      {
        $match: {
          _id: new ObjectId(query)
        },
      },
      {
        $lookup: {
          from: "pedido_trackings",
          localField: "_id", //Nombre del campo de la coleccion actual
          foreignField: "pedido", //Nombre del campo de la coleccion a relacionar
          as: "pedido_trackings", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
      {
        $lookup: {
          from: "reportepedidos",
          localField: "_id", //Nombre del campo de la coleccion actual
          foreignField: "idPedido", //Nombre del campo de la coleccion a relacionar
          as: "reporte_pedidos", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
      {
        $lookup: {
          from: "puntos_ganados_establecimientos",
          localField: "_id", //Nombre del campo de la coleccion actual
          foreignField: "pedido", //Nombre del campo de la coleccion a relacionar
          as: "puntos_ganados", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
    ]).exec(callback);
  },
  /** Pedidos  por mes de un distribuidor */
  getTop10ProdDistMesApp: function (query, callback) {

    const idDistribuidor = query;
    let ObjectId = require("mongoose").Types.ObjectId;
    this.aggregate([
      {
        $match: {
          distribuidor: new ObjectId(idDistribuidor),
          $or: [
            { estado: "Entregado" },
            { estado: "Recibido" },
            { estado: "Calificado" },
          ],
        },
      },
      {
        $lookup: {
          from: "reportepedidos",
          localField: "productos.product", //Nombre del campo de la coleccion actual
          foreignField: "productoId", //Nombre del campo de la coleccion a relacionar
          pipeline: [
            {
              $addFields: {
                costoTotalProducto: {
                  $multiply: ["$unidadesCompradas", "$costoProductos"], // Precio final
                },
              },
            },
            {
              $project: {
                productoId: "$productoId",
                nombreProducto: "$nombreProducto",
                costoTotalProducto: "$costoTotalProducto",
                fotos: "$detalleProducto.fotos",
              },
            },
          ],
          as: "data_detalle_pedido", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
      {
        $unwind: "$data_detalle_pedido",
      },
      {
        $replaceRoot: {
          newRoot: "$data_detalle_pedido",
        },
      },
      {
        $group: {
          _id: "$productoId",
          costoTotalProducto: { $sum: "$costoTotalProducto" },
        },
      },
      {
        $sort: { costoTotalProducto: -1 },
      },
      {
        $limit: 10,
      },
      {
        $lookup: {
          from: "productos",
          localField: "_id", //Nombre del campo de la coleccion actual
          foreignField: "_id", //Nombre del campo de la coleccion a relacionar
          as: "data_productos", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
      {
        $addFields: {
          costoTotalProducto: "$data_productos.costoTotalProducto",
        },
      },
      {
        $unwind: "$data_productos",
      },
      {
        $replaceRoot: {
          newRoot: "$data_productos",
        },
      },
      {
        $project: {
          fotos: "$fotos",
          estado: "$estadoActualizacion",
          nombre: "$nombre",
          descripcion: "$descripcion",
          precios: "$precios",
          sku: "$codigo_distribuidor_producto"
        }
      }
    ]).exec(callback);
  },
  /** TABLA pedidos distribuidor */
  getInformeDistribuidorTablaPedidos2: function (req, callback) {
    this.aggregate([
      {
        $match: req.query,
      },
      {
        $sort: {
          createdAt: -1
        }
      },
      {
        $skip: req.skip
      },
      {
        $limit: 10
      },
      {
        $lookup: {
          from: horecaBD.collection.name, //Nombre de la colecccion a relacionar
          localField: "usuario_horeca", //Nombre del campo de la coleccion actual
          foreignField: "_id", //Nombre del campo de la coleccion a relacionar
          as: "data_horeca", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
      {
        $lookup: {
          from: "punto_entregas",
          localField: "punto_entrega", //Nombre del campo de la coleccion actual
          foreignField: "_id", //Nombre del campo de la coleccion a relacionar
          as: "data_punto", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
      {
        $lookup: {
          from: "codigos_descuento_generados",
          localField: "_id", //Nombre del campo de la coleccion actual
          foreignField: "pedidos.pedido", //Nombre del campo de la coleccion a relacionar
          as: "data_cod_descuento", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
      {
        $lookup: {
          from: "distribuidores_vinculados",
          localField: "punto_entrega", //Nombre del campo de la coleccion actual
          foreignField: "punto_entrega", //Nombre del campo de la coleccion a relacionar
          pipeline: [
            {
              $match: req.query,
            },
            {
              $lookup: {
                from: trabajadoresBD.collection.name, //Nombre de la colecccion a relacionar
                localField: "vendedor", //Nombre del campo de la coleccion actual
                foreignField: "_id", //Nombre del campo de la coleccion a relacionar
                as: "data_trabajador", //Nombre del campo donde se insertara todos los documentos relacionados
              },
            },
          ],
          as: "data_vinculacion", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
      {
        $lookup: {
          from: "pedido_trackings",
          localField: "_id", //Nombre del campo de la coleccion actual
          foreignField: "pedido", //Nombre del campo de la coleccion a relacionar
          pipeline: [
            {
              $facet: {
                fecha_aprobado: [
                  {
                    $match: { estado_nuevo: "Aprobado Externo" },
                  },
                  {
                    $project: {
                      estado_aprobado: "$estado_nuevo",
                      fecha_aprobado: "$createdAt",
                    },
                  },
                ],
                fecha_entrega: [
                  {
                    $match: { estado_nuevo: "Entregado" },
                  },
                  {
                    $project: {
                      estado_entrega: "$estado_nuevo",
                      fecha_entrega: "$createdAt",
                    },
                  },
                ],
              },
            },
          ],
          as: "data_tracking", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
      {
        $addFields: {
          pedido_fecha_aprobado: {
            $arrayElemAt: ["$data_tracking.fecha_aprobado.fecha_aprobado", 0],
          },
          pedido_fecha_entregado: {
            $arrayElemAt: ["$data_tracking.fecha_entrega.fecha_entrega", 0],
          },
        },
      },
      {
        $project: {
          establecimiento: {
            $arrayElemAt: ["$data_horeca.nombre_establecimiento", 0],
          },
          tipo_persona: {
            $arrayElemAt: ["$data_horeca.tipo_usuario", 0],
          },
          nit: { $arrayElemAt: ["$data_horeca.nit", 0] },
          pais: { $arrayElemAt: ["$data_horeca.pais", 0] },
          departamento: {
            $arrayElemAt: ["$data_punto.departamento", 0],
          },
          ciudad: { $arrayElemAt: ["$data_punto.ciudad", 0] },
          punto_entrega: { $arrayElemAt: ["$data_punto.nombre", 0] },
          tipo_negocio: {
            $arrayElemAt: ["$data_horeca.tipo_negocio", 0],
          },
          pedido_fecha: "$createdAt",
          nro_pedido: "$id_pedido",
          estado_actual: "$estado",
          fecha_aprobado: {
            $arrayElemAt: ["$pedido_fecha_aprobado", 0],
          },
          fecha_entregado: {
            $arrayElemAt: ["$pedido_fecha_entregado", 0],
          },
          valor_pedido: "$total_pedido",
          numero_referencias: { $size: "$productos" },
          puntos_ganados: "$puntos_ganados",
          puntos_redimidos: "$puntos_redimidos",
          estado_codigo: {
            $arrayElemAt: ["$data_cod_descuento.estado", 0],
          },
          codigo_descuento: {
            $arrayElemAt: ["$data_cod_descuento.codigo_creado", 0],
          },
          equipo_comercial: "$data_vinculacion",
          _id: "$_id",
        },
      },
      {
        $sort: { pedido_fecha: -1 },
      },
    ]).exec(callback);
  },
};
const Pedido = (module.exports = mongoose.model("Pedido", PedidoSchema));