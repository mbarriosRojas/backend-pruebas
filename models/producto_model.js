const mongoose = require("mongoose");
const marcaBD = require("./marca_producto_model");
const lineaBD = require("./linea_producto_model");
const categoriaBD = require("./categoria_producto_model");
const organizacionBD = require("./organizacion_model");
const productosBD = require("./producto_model");

const pedidosDB = require("./categoria_producto_model");
const moment = require("moment");
const { Decimal128 } = mongoose.Schema.Types;

// Schema

const ProductoSchema = mongoose.Schema({
  fotos: [
    {
      type: String,
      required: false,

    },
  ],
  nombre: { type: String, required: true },
  descripcion: { type: String, required: false, default: "" },
  precios: [
    {
      unidad_medida: { type: String, required: false },
      cantidad_medida: { type: Number, required: false },
      estado: {
        type: String,
        required: false,
        default: "Disponible",
        enum: ["Disponible", "Agotado"],
      },
      precio_unidad: { 
        type: Number,
        default: 0,
        set: (v) => typeof v === 'string' ? parseFloat(v.replace(",", ".")) : v,
      },
      precio_caja: { 
        type: Number,
        default: 0,
        set: (v) => typeof v === 'string' ? parseFloat(v.replace(",", ".")) : v,
      },
      precio_descuento: { 
        type: Number,
        default: 0,
        set: (v) => typeof v === 'string' ? parseFloat(v.replace(",", ".")) : v,
      },
      inventario_unidad: { type: Number, required: false, default: 0 },
      inventario_caja: { type: Number, required: false, default: 0 },
      puntos_ft_unidad: { type: Number, required: false, default: 0 },
      puntos_ft_caja: { type: Number, required: false, default: 0 },
      und_x_caja: { type: Number, required: false, default: 0 },
      unidad_medida_manual: { type: String, required: false },
    },
  ],
  estadoActualizacion: {
    type: String,
    required: false,
    default: "Pendiente",
    enum: ["Pendiente", "Administrador", "Aceptado", "Rechazado", "Inactivo"],
  },
  fecha_vencimiento: { type: Date, required: false },
  fecha_cierre_puntosft: { type: Date, required: false },
  fecha_apertura_puntosft: { type: Date, required: false },
  promocion: { type: Boolean, required: false, default: false },
  saldos: { type: Boolean, required: false, default: false },
  codigo_ft: { type: String, required: false },
  codigo_promo: { type: String, required: false },
  codigo_distribuidor: {
    type: mongoose.Schema.Types.ObjectId,
    ref: "Distribuidor",
    required: false,
  },
  codigo_distribuidor_producto: { type: String, required: false },
  codigo_organizacion_producto: { type: String, required: false },
  codigo_organizacion: {
    type: mongoose.Schema.Types.ObjectId,
    ref: "Organizacion",
    required: false,
  },
  marca_producto: {
    type: mongoose.Schema.Types.ObjectId,
    ref: "Marca_producto",
    required: false,
  },
  categoria_producto: {
    type: mongoose.Schema.Types.ObjectId,
    ref: "Categoria_producto",
    required: false,
  },
  linea_producto: {
    type: [mongoose.Schema.Types.ObjectId],
    ref: "Linea_producto",
    required: false,
  },
  productos_promocion: {
    type: [mongoose.Schema.Types.ObjectId],
    ref: "Producto",
    required: false,
  },
  productos_promocion_inventario_unidades: {
    type: [Number],
    required: false,
  },
  ficha_tecnica: { type: String, required: false, default: "" },
  fechaAceptado: { type: Date, required: false },
  prodBiodegradable: { type: Boolean, required: false, default: false },
  prodPedido: { type: Boolean, required: false, default: false },
  prodDescuento: { type: Boolean, required: false, default: false },
  prodPorcentajeDesc: {
    type: Number,
    default: 0,
    set: (v) => typeof v === 'string' ? parseFloat(v.replace(",", ".")) : v,
  },
  mostrarPF: { type: Boolean, required: false, default: false },
  comentarioVencimiento: { type: String, required: false, default: "" },
  organizacion_manual: { type: String, required: false },
  marca_manual: { type: String, required: false },
  categoria_manual: { type: String, required: false },
  linea_manual: { type: String, required: false },
  establecimientos_interesados: {
    type: [mongoose.Schema.Types.ObjectId],
    ref: "Usuario_horeca",
    required: false,
  },
  descuentosEspeciales: [],
  createdAt: { type: Date, required: false, default: Date.now },
});

ProductoSchema.statics = {
  get: function (query, callback) {
    this.findOne(query)
      .populate("marca_producto")
      .populate("categoria_producto")
      .populate("linea_producto")
      .populate("marca_producto")
      .populate("productos_promocion")
      .populate({
        path: "productos_promocion",
        populate: { path: "marca_producto" },
      })
      .exec(callback);
  },
  getOneByNombreYCodigoProducto: function (query, callback) {
    this.findOne(query).exec(callback);
  },
  getAllCronTab: function (query, callback) {
    let busqueda = {
      $or: [
        {
          estadoActualizacion: "Aceptado",
        },
        {
          estadoActualizacion: "Administrador",
        },
        { estadoActualizacion: "Inactivo" },
      ],
      saldos: false,
      promocion: false,
      precios: { $elemMatch: { puntos_ft_unidad: { $gt: 0 } } },
    };

    this.find(busqueda)
      .select(
        "_id fecha_cierre_puntosft fecha_apertura_puntosft codigo_organizacion codigo_ft, precios"
      )
      .exec(callback);
  },
  getAllCronTabAll: function (query, callback) {
    this.find()
      .select(
        "_id fecha_cierre_puntosft fecha_apertura_puntosft codigo_organizacion codigo_ft, precios"
      )
      .exec(callback);
  },
  getAllVinculaciones: function (query, callback) {
    var ObjectId = require("mongoose").Types.ObjectId;
    const consulta = {
      codigo_distribuidor: new ObjectId(query),
      estadoActualizacion: "Aceptado"
    };
    this.find(consulta)
      .exec(callback);
  },
  getAll: function (query, callback) {
    this.find(query)
      .populate("marca_producto")
      .populate("categoria_producto")
      .populate("linea_producto")
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
  deleteDistribuidor: function (query, callback) {
    this.remove(query).exec(callback);
  },
  deleteSol: function (query, callback) {
    this.remove(query).exec(callback);
  },
  create: function (data, callback) {
    const Producto = new this(data);
    Producto.save(callback);
  },
  getProductoCompleto: function (id, callback) {
    var ObjectId = require("mongoose").Types.ObjectId;
    const query = {
      _id: new ObjectId(id),
    };
    this.find(query)
      .populate("marca_producto")
      .populate("categoria_producto")
      .populate("linea_producto")
      .populate("codigo_distribuidor")
      .exec(callback);
  },
  getProductoDetallado: function (query, callback) {
    this.find(query).populate("precios").exec(callback);
  },
  getProductoValidacion: function (obj, callback) {
    var ObjectId = require("mongoose").Types.ObjectId;
    const query = { _id: new ObjectId(obj._id) };
    this.find(query).populate("precios").exec(callback);
  },
  getProductoPromocion: function (id, callback) {
    var ObjectId = require("mongoose").Types.ObjectId;
    const query = { _id: new ObjectId(id) };
    this.findOne(query)
      .populate("precios")
      .populate("productos_promocion")
      .exec(callback);
  },
  getProductosPorLineaPorDistribuidor: function (obj, callback) {
    var ObjectId = require("mongoose").Types.ObjectId;
    const query = {
      codigo_distribuidor: new ObjectId(obj.codigo_distribuidor),
      linea_producto: { $in: [ObjectId(obj.linea_producto)] },
    };
    this.find(query).exec(callback);
  },
  getProductosPorDistribuidor: function (obj, callback) {
    var ObjectId = require("mongoose").Types.ObjectId;
    const query = {
      codigo_distribuidor: new ObjectId(obj.codigo_distribuidor),
    };
    this.find(query).populate("linea_producto").exec(callback);
  },
  indicadoresOrgaizaciones: function (_idOrganizacion, callback) {
    var ObjId = require("mongoose").Types.ObjectId;
    this.aggregate([
      {
        $match: {
          codigo_organizacion: new ObjId("6245c88dd93f7e5d7cd80a56"),
        },
      },
      {
        $lookup: {
          from: "reportepedidos", //Nombre de la colecccion a relacionar
          localField: "_id", //Nombre del campo de la coleccion actual
          foreignField: "productoId", //Nombre del campo de la coleccion a relacionar
          as: "dataPedido", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
      {
        $lookup: {
          from: "distribuidors", //Nombre de la colecccion a relacionar
          localField: "codigo_distribuidor", //Nombre del campo de la coleccion actual
          foreignField: "_id", //Nombre del campo de la coleccion a relacionar
          as: "dataDistribuidor", //Nombre del campo donde se insertara todos los documentos relacionados
          pipeline: [
            {
              $lookup: {
                from: "distribuidores_vinculados", //Nombre de la colecccion a relacionar
                localField: "_id", //Nombre del campo de la coleccion actual
                foreignField: "distribuidor", //Nombre del campo de la coleccion a relacionar
                as: "data_distribuidores_vinculados", //Nombre del campo donde se insertara todos los documentos relacionados
                pipeline: [
                  {
                    $lookup: {
                      from: "punto_entregas", //Nombre de la colecccion a relacionar
                      localField: "punto_entrega", //Nombre del campo de la coleccion actual
                      foreignField: "_id", //Nombre del campo de la coleccion a relacionar
                      as: "data_punto_entrega", //Nombre del campo donde se insertara todos los documentos relacionados
                    },
                  },
                ],
              },
            },
          ],
        },
      },
      /*{
        $project: {
         
          total_planeadores: {
            $sum: { $size: "$usuariosPlaneadores" },
          },
        },
      },*/
      /*
      {
        $lookup: {
          from: lineaBD.collection.name, //Nombre de la colecccion a relacionar
          localField: "linea_producto", //Nombre del campo de la coleccion actual
          foreignField: "_id", //Nombre del campo de la coleccion a relacionar
          as: "linea_data", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
      {
        $lookup: {
          from: categoriaBD.collection.name, //Nombre de la colecccion a relacionar
          localField: "categoria_producto", //Nombre del campo de la coleccion actual
          foreignField: "_id", //Nombre del campo de la coleccion a relacionar
          as: "categoria_data", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
      {
        $lookup: {
          from: "distribuidors", //Nombre de la colecccion a relacionar
          localField: "codigo_distribuidor", //Nombre del campo de la coleccion actual
          foreignField: "_id", //Nombre del campo de la coleccion a relacionar
          as: "organizacion_data", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },*/
    ]).exec(callback);
  },
  productosPorOrg: function (_idOrganizacion, callback) {
    var ObjId = require("mongoose").Types.ObjectId;
    this.aggregate([
      {
        $match: {
          codigo_organizacion: new ObjId(_idOrganizacion),
        },
      },
      {
        $lookup: {
          from: marcaBD.collection.name, //Nombre de la colecccion a relacionar
          localField: "marca_producto", //Nombre del campo de la coleccion actual
          foreignField: "_id", //Nombre del campo de la coleccion a relacionar
          as: "marca_productos_query", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
      {
        $lookup: {
          from: lineaBD.collection.name, //Nombre de la colecccion a relacionar
          localField: "linea_producto", //Nombre del campo de la coleccion actual
          foreignField: "_id", //Nombre del campo de la coleccion a relacionar
          as: "linea_productos_query", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
      {
        $lookup: {
          from: categoriaBD.collection.name, //Nombre de la colecccion a relacionar
          localField: "categoria_producto", //Nombre del campo de la coleccion actual
          foreignField: "_id", //Nombre del campo de la coleccion a relacionar
          as: "cat_productos_query", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
      {
        $lookup: {
          from: organizacionBD.collection.name, //Nombre de la colecccion a relacionar
          localField: "codigo_organizacion", //Nombre del campo de la coleccion actual
          foreignField: "_id", //Nombre del campo de la coleccion a relacionar
          as: "organizacion_query", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
      {
        $lookup: {
          from: "distribuidors", //Nombre de la colecccion a relacionar
          localField: "codigo_distribuidor", //Nombre del campo de la coleccion actual
          foreignField: "_id", //Nombre del campo de la coleccion a relacionar
          as: "distribuidor_query", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
    ]).exec(callback);
  },
  getSolicitudes: function (query, callback) {
    this.aggregate([
      {
        $match: {
          promocion: false,
          saldos: false,
          $or: [
            { estadoActualizacion: "Pendiente" },
            { estadoActualizacion: "Rechazado" },
            { estadoActualizacion: "Rechazado" },
          ],
        },
      },
      {
        $sort: { createdAt: -1 },
      },
      {
        $lookup: {
          from: marcaBD.collection.name, //Nombre de la colecccion a relacionar
          localField: "marca_producto", //Nombre del campo de la coleccion actual
          foreignField: "_id", //Nombre del campo de la coleccion a relacionar
          as: "marca_producto_query", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
      {
        $lookup: {
          from: lineaBD.collection.name, //Nombre de la colecccion a relacionar
          localField: "linea_producto", //Nombre del campo de la coleccion actual
          foreignField: "_id", //Nombre del campo de la coleccion a relacionar
          as: "lista_producto_query", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
      {
        $lookup: {
          from: categoriaBD.collection.name, //Nombre de la colecccion a relacionar
          localField: "categoria_producto", //Nombre del campo de la coleccion actual
          foreignField: "_id", //Nombre del campo de la coleccion a relacionar
          as: "categoria_producto_query", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
      {
        $lookup: {
          from: "distribuidors", //Nombre de la colecccion a relacionar
          localField: "codigo_distribuidor", //Nombre del campo de la coleccion actual
          foreignField: "_id", //Nombre del campo de la coleccion a relacionar
          as: "data_distribuidor", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
      {
        $facet: {
          estadosAgrupados: [
            {
              $group: {
                _id: "$estadoActualizacion",
                cantidad: { $sum: 1 },
              },
            },
          ],
          dataProd: [
            {
              $project: {
                _id: "$_id",
                estado: "$estadoActualizacion",
                codigoFeat: "$codigo_ft",
                codigoOrganizacion: "$codigo_organizacion",
                codigoDistribuidor: "$codigo_distribuidor",
                codigoOrganizacionProducto: "$codigo_organizacion_producto",
                codigoDistribuidorProducto: "$codigo_distribuidor_producto",
                saldos: "$saldos",
                nombreDistribuidor: "$data_distribuidor.nombre",
                nitDistribuidor: "$data_distribuidor.nit_cc",
                fotos: "$fotos",
                categoria: "$categoria_producto_query",
                marca: "$marca_producto_query",
                linea: "$lista_producto_query",
                unidadMedida: "$precios",
                nombreProducto: "$nombre",
                precios: "$precios",
                organizacion_manual: "$organizacion_manual",
                marca_manual: "$marca_manual",
                categoria_manual: "$categoria_manual",
                linea_manual: "$linea_manual",
              },
            },
          ],
        },
      },
    ]).exec(callback);
  },
  graficaProductosxMes: function (query, callback) {
    this.aggregate([
      {
        $match: {},
      },
      {
        $sort: { createdAt: -1 },
      },
    ]).exec(callback);
  },
  getGraficasMarcasPruductosMeses: function (query, callback) {
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
        $lookup: {
          from: marcaBD.collection.name, //Nombre de la colecccion a relacionar
          localField: "marca_producto", //Nombre del campo de la coleccion actual
          foreignField: "_id", //Nombre del campo de la coleccion a relacionar
          as: "marca_productos_query", //Nombre del campo donde se insertara todos los documentos relacionados
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
  getCategoriasGeneralesPorDistribuidor: function (req, callback) {
    var ObjectId = require("mongoose").Types.ObjectId;
    const query = { codigo_distribuidor: new ObjectId(req.trim()) };
    this.aggregate([
      {
        $match: query,
      },
      {
        $lookup: {
          from: categoriaBD.collection.name, //Nombre de la colecccion a relacionar
          localField: "categoria_producto", //Nombre del campo de la coleccion actual
          foreignField: "_id", //Nombre del campo de la coleccion a relacionar
          as: "data_categorias", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
      {
        $unwind: "$data_categorias",
      },
      {
        $replaceRoot: {
          newRoot: "$data_categorias",
        },
      },
    ]).exec(callback);
  },
  getLineasGeneralesPorDistribuidor: function (req, callback) {
    var ObjectId = require("mongoose").Types.ObjectId;
    const query = { codigo_distribuidor: new ObjectId(req.trim()) };
    this.aggregate([
      {
        $match: query,
      },
      {
        $lookup: {
          from: lineaBD.collection.name, //Nombre de la colecccion a relacionar
          localField: "linea_producto", //Nombre del campo de la coleccion actual
          foreignField: "_id", //Nombre del campo de la coleccion a relacionar
          as: "data_lineas", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
      {
        $unwind: "$data_lineas",
      },
      {
        $replaceRoot: {
          newRoot: "$data_lineas",
        },
      },
    ]).exec(callback);
  },
  countProductos: function (req, callback) {
    this.find({}).exec(callback);
  },
  getAllProdPrecio: function (req, callback) {
    this.aggregate([
      {
        $match: { codigo_ft: req },
      },
      {
        $lookup: {
          from: "distribuidors", //Nombre de la colecccion a relacionar
          localField: "codigo_distribuidor", //Nombre del campo de la coleccion actual
          foreignField: "_id", //Nombre del campo de la coleccion a relacionar
          as: "data_distribuidor", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
      {
        $project: {
          _id: "$_id",
          estado: "$estadoActualizacion",
          codigoFeat: "$codigo_ft",
          codigoDistribuidor_producto: "$codigo_distribuidor_producto",
          nombreProducto: "$nombre",
          distribuidor: "$data_distribuidor",
          unidadMedida: "$precios.unidad_medida",
          cantMedida: "$precios.cantidad_medida",
          presentacionCaja: "$precios.und_x_caja",
          precioCaja: "$precios.precio_caja",
          precioUnidad: "$precios.precio_unidad",
          cantMedida: "$precios.cantidad_medida",
          fechaAceptado: "$fechaAceptado",
        },
      },
    ]).exec(callback);
  },
  getProductos_busqueda: function (req, callback) {
    var ObjectId = require("mongoose").Types.ObjectId;
    this.aggregate([
      {
        $match: {
          codigo_distribuidor: new ObjectId(req),
          estadoActualizacion: "Pendiente",
        },
      },
    ]).exec(callback);
  },
  obtenerInventarioPrecios: function (query, callback) {
    var ObjectId = require("mongoose").Types.ObjectId;
    const d = new Date();
    const fecha_referencia = d.setMonth(d.getMonth() - 3);
    this.aggregate([
      {
        $match: {
          codigo_distribuidor: new ObjectId(query._id),
          //estadoActualizacion: "Aceptado",
          saldos: false,
          promocion: false,
        },
      },
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
          from: "linea_productos", //Nombre de la colecccion a relacionar
          localField: "linea_producto", //Nombre del campo de la coleccion actual
          foreignField: "_id", //Nombre del campo de la coleccion a relacionar
          as: "data_linea", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
      {
        $lookup: {
          from: "categoria_productos", //Nombre de la colecccion a relacionar
          localField: "categoria_producto", //Nombre del campo de la coleccion actual
          foreignField: "_id", //Nombre del campo de la coleccion a relacionar
          as: "data_categoria", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
      {
        $lookup: {
          from: "organizacions", //Nombre de la colecccion a relacionar
          localField: "codigo_organizacion", //Nombre del campo de la coleccion actual
          foreignField: "_id", //Nombre del campo de la coleccion a relacionar
          as: "data_organizacion", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
      /** Busca las transacciones entre punto y dist */
      {
        $lookup: {
          from: "reportepedidos",
          localField: "_id", //Nombre del campo de la coleccion actual
          foreignField: "productoId", //Nombre del campo de la coleccion a relacionar
          pipeline: [
            {
              $match: {
                createdAt: {
                  $gte: new Date(fecha_referencia),
                },
              },
            },
            {
              $facet: {
                puntos_alcanzados: [
                  {
                    $group: {
                      _id: "$idPunto",
                      total: { $sum: 1 },
                    },
                  },
                ],
                pedidos_valor: [
                  {
                    $addFields: {
                      costoTotalProducto: {
                        $multiply: ["$unidadesCompradas", "$costoProductos"],
                      },
                    },
                  },
                  {
                    $group: {
                      _id: "$productoId",
                      total: { $sum: "$costoTotalProducto" },
                    },
                  },
                ],
              },
            },
            {
              $project: {
                puntos_alcanzados: { $size: "$puntos_alcanzados" },
                pedidos_valor: {
                  $arrayElemAt: ["$pedidos_valor.total", 0],
                },
              },
            },
          ],
          as: "data_compras",
        },
      },
      {
        $addFields: {
          cajas_cant: {
            $arrayElemAt: ["$precios.inventario_caja", 0],
          }
        }
      },
      {
        $project: {
          _id: "$_id",
          idDistribuidores: "$distribuidor_query._id",
          producto_id: "$_id",
          codigo_ft: "$codigo_ft",
          codigo_organizacion_producto: "$codigo_organizacion_producto",
          codigo_distribuidor_producto: "$codigo_distribuidor_producto",
          nombre_producto: "$nombre",
          estado: "$estadoActualizacion",
          fotos: "$fotos",
          marca: { $arrayElemAt: ["$data_marca.nombre", 0] },
          organizacion: { $arrayElemAt: ["$data_organizacion.nombre", 0] },
          categoria_prod: { $arrayElemAt: ["$data_categoria.nombre", 0] },
          linea_prod: { $arrayElemAt: ["$data_linea.nombre", 0] },
          descripcion: "$descripcion",
          mostrarPF: "$mostrarPF",

          cantidad_medida: {
            $arrayElemAt: ["$precios.cantidad_medida", 0],
          },
          unidad_medida: {
            $arrayElemAt: ["$precios.precio_descuento", 0],
          },
          precio_descuento: {
            $arrayElemAt: ["$precios.precio_unidad", 0],
          },
          precio_venta_x_und: {
            $arrayElemAt: ["$precios.precio_unidad", 0],
          },
          precio_venta_x_caja: {
            $arrayElemAt: ["$precios.precio_caja", 0],
          },
          puntos_ft: {
            $arrayElemAt: ["$precios.puntos_ft_unidad", 0],
          },
          und_x_caja: {
            $arrayElemAt: ["$precios.und_x_caja", 0],
          },
          unidades: {
            $arrayElemAt: ["$precios.inventario_unidad", 0],
          },
          cajas: '$cajas_cant',
          establecimientos: {
            $arrayElemAt: ["$data_compras.puntos_alcanzados", 0],
          },
          ventas_prod: { $arrayElemAt: ["$data_compras.pedidos_valor", 0] },
          precios: "$precios",
          prodBiodegradable: {
            $cond: {
              if: {
                $eq: ['$prodBiodegradable', true],
              },
              then: 'si',
              else: 'no',
            },
          },
          // "$prodBiodegradable",
          prodPedido: {
            $cond: {
              if: {
                $eq: ['$prodPedido', true],
              },
              then: 'si',
              else: 'no',
            },
          },
          prodDescuento: {
            $cond: {
              if: {
                $eq: ['$prodDescuento', true],
              },
              then: 'si',
              else: 'no',
            },
          },
          prodPorcentajeDesc: "$prodPorcentajeDesc",
          accion: null,
        },
      },
      /** Se agrega este campo para un sort no-case-sensitive */
      {
        $addFields: {
          label_to_sort: {
            $toLower: "$nombre_producto",
          },
        },
      },
      {
        $sort: { label_to_sort: 1 },
      },
    ]).exec(callback);
  },

  /** Trae toda la informacion del componente de puntos-feat de una organizacion */
  getDataComponentePuntosFeatNew2: function (query, callback) {
    var ObjectId = require("mongoose").Types.ObjectId;
    this.aggregate([
      {
        $match: {
          codigo_organizacion: new ObjectId(query.idOrg),
        },
      },
      {
        $facet: {
          puntosActuales: [
            {
              $match: {
                'precios.puntos_ft_unidad': {
                  $gt: 0,
                },
                /* fecha_apertura_puntosft: {
                    $gte: new Date(query.inicio),
                  },
                   fecha_cierre_puntosft: {
                    $lte: new Date(query.fin),
                  }, */

              },
            },
            {
              $project: {
                idProducto: '$_id',
                nombreProducto: '$nombre',
                fechainicioPunto: '$fecha_apertura_puntosft',
                fechaFinPunto: '$fecha_cierre_puntosft',
                puntosOfrecidos: '$precios.puntos_ft_unidad',
                puntosFTConsumidos: '',
                cantidadCompradas: '',
                marcaProducto: '$marcaProducto',
                categoriaProducto: '$categoria_producto'
              }
            }
          ],
          historicoPedidos: [
            {
              $lookup: {
                from: "reportepedidos",
                localField: "_id",
                foreignField: "productoId",
                as: "hitoricoPedidos",
                pipeline: [
                  {
                    $match: {
                      puntos_ft_ganados: {
                        $gt: 0,
                      },

                      /* createdAt: {
                          $gte: new Date(query.inicio),
                          $lte: new Date(query.fin),
                        }, */

                    },
                  },
                ]
              },
            },
            {
              $unwind: "$hitoricoPedidos",
            },
            {
              $replaceRoot: {
                newRoot: "$hitoricoPedidos",
              },
            },
            {
              $project: {
                idProducto: '$productoId',
                nombreProducto: '$detalleProducto.nombre',
                puntosOfrecidos: '$puntos_ft_unidad',
                puntosFTConsumidos: '$puntos_ft_ganados',
                cantidadCompradas: '$unidadesCompradas',
                marcaProducto: '$marcaProducto',
                categoriaProducto: '$categoriaProducto',

                fechainicioPunto: {
                  $toDate: "$detalleProducto.fecha_apertura_puntosft"
                },
                fechaFinPunto: {
                  $toDate: "$detalleProducto.fecha_cierre_puntosft"
                },
              }
            },
            {
              $group: {
                _id: {
                  nombreProducto: "$nombreProducto",
                  fechainicio: "$fechainicioPunto",
                  fechafin: "$fechaFinPunto",
                },

                idProducto: {
                  $first: '$idProducto'
                },
                nombreProducto: {
                  $first: '$nombreProducto'
                },
                fechainicioPunto: {
                  $first: '$fechainicioPunto'
                },
                fechaFinPunto: {
                  $first: '$fechaFinPunto'
                },
                puntosOfrecidos: {
                  $first: '$puntosOfrecidos'
                },
                puntosFTConsumidos: {
                  $sum: '$puntosFTConsumidos'
                },
                cantidadCompradas: {
                  $sum: '$cantidadCompradas'
                },
                marcaProducto: {
                  $first: '$marcaProducto'
                },
                catProducto: {
                  $first: '$categoriaProducto'
                },
              },
            },
          ],
        },
      },
      {
        $project: {
          combine: {
            $concatArrays: [
              "$puntosActuales",
              "$historicoPedidos"
            ]
          }
        }
      },
      {
        $unwind: "$combine",
      },
      {
        $replaceRoot: {
          newRoot: "$combine",
        },
      },
      {
        $lookup: {
          from: "marca_productos",
          localField: "marcaProducto",
          foreignField: "_id",
          as: "dataMarca",
        },
      },
      {
        $lookup: {
          from: "categoria_productos",
          localField: "catProducto",
          foreignField: "_id",
          as: "dataCat",
        },
      },
      {
        $group: {
          _id: {
            fechainicio: "$fechainicioPunto",
            fechafin: "$fechaFinPunto",
            nombreProducto: "$nombreProducto",
          },

          idProducto: {
            $first: '$idProducto'
          },
          nombreProducto: {
            $first: '$nombreProducto'
          },
          nombreProducto: {
            $first: '$nombreProducto'
          },
          fechaFinPunto: {
            $first: '$fechaFinPunto'
          },
          fechainicioPunto: {
            $first: '$fechainicioPunto'
          },
          puntosOfrecidos: {
            $first: '$puntosOfrecidos'
          },
          puntosFTConsumidos: {
            $sum: '$puntosFTConsumidos'
          },
          cantidadCompradas: {
            $sum: '$cantidadCompradas'
          },
          nombreMarca: {
            $first: '$dataMarca.nombre'
          },
          idMarca: {
            $first: '$dataMarca._id'
          },
          nombreCat: {
            $first: '$dataCat.nombre'
          },
          idCat: {
            $first: '$dataCat._id'
          }



        },
      },
    ]).exec(callback);
  },
  getfeatMixHibrido: function (query, callback) {
    var ObjectId = require("mongoose").Types.ObjectId;
    this.aggregate([
      {
        $match: {
          estadoActualizacion: 'Aceptado',
        },
      },

      {
        $sort: { 'precios.precio_unidad': 1 },
      },
      //                  'productosDescuento.productoID': new ObjectId('$_id')
      {
        $lookup: {
          from: "productos_descuento_distribuidors",
          localField: "codigo_distribuidor",
          foreignField: "idDistribuidor",
          as: "dataEspeciales",
          pipeline: [

            {
              $match: {
                idPunto: new ObjectId(query.idPunto)
              },
            },
            {
              $unwind: "$productosDescuento",
            },
            {
              $replaceRoot: {
                newRoot: "$productosDescuento",
              },
            },
          ]
        },
      },
      {
        $lookup: {
          from: "distribuidores_vinculados",
          localField: "codigo_distribuidor",
          foreignField: "distribuidor",
          as: "dataVinculacion",
          pipeline: [
            {
              $match: {
                punto_entrega: new ObjectId(query.idPunto)
              },
            },
          ]
        },
      },
      {
        $lookup: {
          from: "distribuidors",
          localField: "codigo_distribuidor",
          foreignField: "_id",
          as: "dataZona",
          pipeline: [
            {
              $match: {
                solicitud_vinculacion: "Aprobado",
                zonas_cobertura: {
                  $geoIntersects: {
                    $geometry: {
                      type: "Point",
                      coordinates: [parseFloat(query.long), parseFloat(query.lat)],
                    },
                  },
                },
              }
            }
          ]
        },
      },
      {
        $lookup: {
          from: "documentos_usuario_solicitantes",
          localField: "codigo_distribuidor",
          foreignField: "usuario_distribuidor",
          as: "dataDocumentos",
        },
      },
      {
        $lookup: {
          from: "distribuidors",
          localField: "codigo_distribuidor",
          foreignField: "_id",
          as: "dataDist",
          pipeline: [
            {
              $match: {
                solicitud_vinculacion: "Aprobado"
              }
            },
            {
              $project: {
                idDistribuidor: '$_id',
                logo: '$logo',
                tiempo_entrega: '$tiempo_entrega',
                ciudad: '$ciudad',
                descripcion: '$descripcion',
                direccion: '$direccion',
                telefono: '$telefono',
                correo: '$correo',
                horario_atencion: '$horario_atencion',
                metodo_pago: '$metodo_pago',
                urlPago: '$urlPago',
                ranking_gen: '$ranking_gen',
                nombreDistribuidor: '$nombre',
                valor_minimo_pedido: '$valor_minimo_pedido',
                metodo_pago: '$metodo_pago',
                vinculado: [],
                dentroZona: [],
                preciosEspeciales: [],
                dataPrecio: []
              },
            },
          ]
        },
      },
      {
        $addFields: {
          'dataDist.paz_salvo': "$dataVinculacion.pazysalvo",
          'dataDist.convenio': "$dataVinculacion.convenio",
          'dataDist.dataPrecio': "$precios",
          'dataDist.bajoPedido': "$prodPedido",
          'dataDist.prodDescuento': "$prodDescuento",
          'dataDist.prodPorcentajeDesc': "$prodPorcentajeDesc",
          'dataDist.preciosEspeciales': "$dataEspeciales",
          'dataDist.sku': "$codigo_distribuidor_producto",
          'dataDist.documentos': '$dataDocumentos',
          'dataDist.vinculado': {
            $cond: { if: { $gt: [{ $size: "$dataVinculacion" }, 0] }, then: true, else: false }
          },
          'dataDist.dentroZona': {
            $cond: { if: { $gt: [{ $size: "$dataZona" }, 0] }, then: true, else: false }
          }
        },
      },
      {
        $lookup: {
          from: "marca_productos",
          localField: "marca_producto",
          foreignField: "_id",
          as: "dataMarca",
        },
      },
      {
        $lookup: {
          from: "linea_productos",
          localField: "linea_producto",
          foreignField: "_id",
          as: "dataLinea",
        },
      },
      {
        $lookup: {
          from: "categoria_productos",
          localField: "categoria_producto",
          foreignField: "_id",
          as: "dataCate",
        },
      },
      {
        $group: {
          _id: '$codigo_ft',
          idProducto: { $first: '$_id' },
          nombre: { $first: '$nombre' },
          fotos: { $first: '$fotos' },
          descripcion: { $first: '$descripcion' },
          marcaProducto: { $first: '$dataMarca.nombre' },
          unidadMedida: { $first: '$precios.unidad_medida' },
          cantidad_medida: { $first: '$precios.cantidad_medida' },
          dataLinea: { $first: '$dataLinea.nombre' },
          categoriaProducto: { $first: '$dataCate.nombre' },
          ficha_tecnica: { $first: '$ficha_tecnica' },
          precioMinimo: { $first: '$precios.precio_unidad' },
          precioMaximo: { $last: '$precios.precio_unidad' },
          prodBiodegradable: { $last: "$prodBiodegradable" },
          mostrarPF: { $last: "$mostrarPF" },
          puntos_ft_unidad: { $last: "$precios.puntos_ft_unidad" },
          und_x_caja: { $last: "$precios.und_x_caja" },
          unidad_medida_manual: { $last: "$precios.unidad_medida_manual" },
          distribuidores: { $push: "$dataDist" },
        },
      },
    ]).exec(callback);
  },
  getDataComponentePuntosFeatNew3: function (query, callback) {
    var ObjectId = require("mongoose").Types.ObjectId;
    const inicioStr = query.inicio;
    const finStr = query.fin;
    const firstDayOfMonth = new Date(inicioStr);
    const lastDayOfMonth = new Date(finStr);

    const filter = {
      codigo_organizacion: new ObjectId(query.idOrg),
      estadoActualizacion: 'Aceptado',
      mostrarPF: true,
      fecha_apertura_puntosft: { $lte: lastDayOfMonth },
      fecha_cierre_puntosft: { $gte: firstDayOfMonth }
  };
    console.log('filter',filter)

    this.aggregate([
      {
        $match: filter
      },
      {
        $facet: {
          puntosActuales: [
            {
              $match: {
                'precios.puntos_ft_unidad': {
                  $gt: 0,
                },
              },
            },
            {
              $project: {
                idProducto: '$_id',
                nombreProducto: '$nombre',
                fechainicioPunto: '$fecha_apertura_puntosft',
                fechaFinPunto: '$fecha_cierre_puntosft',
                puntosOfrecidos: '$precios.puntos_ft_unidad',
                puntosFTConsumidos: '',
                cantidadCompradas: '',
                marcaProducto: '$marca_producto',
                categoriaProducto: '$categoria_producto'
              }
            },
            
          ],
          /*historicoPedidos: [
            {
              $lookup: {
                from: "reportepedidos",
                localField: "_id",
                foreignField: "productoId",
                as: "hitoricoPedidos",
                pipeline: [
                  {
                    $match: {
                      puntos_ft_unidad: { $gt: 0 },
                      $expr: {
                        $and: [
                          { $lte: ["$detalleProducto.fecha_cierre_puntosft", lastDayOfMonth] },
                          { $gte: ["$detalleProducto.fecha_apertura_puntosft", firstDayOfMonth] }
                        ]
                      }
                    }
                  },
                  {
                    $sort: { createdAt: -1 }
                  }
                ]
              }
            },
            {
              $unwind: "$hitoricoPedidos",
            },
            {
              $replaceRoot: {
                newRoot: "$hitoricoPedidos",
              },
            },
            {
              $project: {
                idProducto: '$productoId',
                nombreProducto: '$detalleProducto.nombre',
                fechainicioPunto: {
                  $toDate: "$detalleProducto.fecha_apertura_puntosft"
                },
                fechaFinPunto: {
                  $toDate: "$detalleProducto.fecha_cierre_puntosft"
                },
                puntosOfrecidos: '$puntos_ft_unidad',
                puntosFTConsumidos: '$puntos_ft_ganados',
                cantidadCompradas: '$unidadesCompradas',
                marcaProducto: '$marcaProducto',
                categoriaProducto: '$categoriaProducto',
                createdAt: '$createdAt',
              }
            },
          ],*/
          historicoPedidos: [
            {
              $lookup: {
                from: "reportepedidos",
                localField: "_id",
                foreignField: "productoId",
                as: "hitoricoPedidos",
                pipeline: [
                  {
                    $match: {
                      puntos_ft_unidad: { $gt: 0 },
                      $expr: {
                        $and: [
                          { $lte: ["$createdAt", lastDayOfMonth] }, // Fecha del pedido <= fin de mes
                          { $gte: ["$createdAt", firstDayOfMonth] } // Fecha del pedido >= inicio de mes
                        ]
                      }
                    }
                  },
                  {
                    $sort: { createdAt: -1 } // Ordenar por fecha más reciente primero
                  }
                ]
              }
            },
            
           {
              $unwind: "$hitoricoPedidos",
            },
            {
              $replaceRoot: {
                newRoot: "$hitoricoPedidos",
              },
            },
           
             {
              $project: {
                idProducto: '$productoId',
                nombreProducto: '$detalleProducto.nombre',
                fechainicioPunto: {
                  $toDate: "$detalleProducto.fecha_apertura_puntosft"
                },
                fechaFinPunto: {
                  $toDate: "$detalleProducto.fecha_cierre_puntosft"
                },
                puntosOfrecidos: '$puntos_ft_unidad',
                puntosFTConsumidos: '$puntos_ft_ganados',
                cantidadCompradas: '$unidadesCompradas',
                marcaProducto: '$marcaProducto',
                categoriaProducto: '$categoriaProducto',
                createdAt: '$createdAt',
              }
            },
           
          ],
        },
      },
     {
        $project: {
          combine: {
            $concatArrays: [
              "$puntosActuales",
              "$historicoPedidos"
            ]
          }
        }
      },
      {
        $unwind: "$combine",
      },
      
      {
        $replaceRoot: {
          newRoot: "$combine",
        },
      },
      {
        $lookup: {
          from: "marca_productos",
          localField: "marcaProducto",
          foreignField: "_id",
          as: "dataMarca",
        },
      },
      {
        $lookup: {
          from: "categoria_productos",
          localField: "categoriaProducto",
          foreignField: "_id",
          as: "dataCat",
        },
      },

      {
        $facet: {
          producto: [
            {
              $group: {
                _id: {
                  fechainicio: "$fechainicioPunto",
                  fechafin: "$fechaFinPunto",
                  nombreProducto: "$nombreProducto",
                },
                idProducto: { $first: "$idProducto" },
                nombreProducto: { $first: "$nombreProducto" },
                fechaFinPunto: { $first: "$fechaFinPunto" },
                fechainicioPunto: { $first: "$fechainicioPunto" },
                puntosOfrecidos: { $first: "$puntosOfrecidos" },
                puntosFTConsumidos: { $sum: "$puntosFTConsumidos" },
                cantidadCompradas: { $sum: "$cantidadCompradas" },
                nombreMarca: { $first: "$dataMarca.nombre" },
                idMarca: { $first: "$dataMarca._id" },
                nombreCat: { $first: "$dataCat.nombre" },
                fechaCreado: { $first: "$createdAt" },
                idCat: { $first: "$dataCat._id" }
              },
            }
          ],
          marca: [
            {
              $group: {
                _id: "$dataMarca._id",
                puntosOfrecidos: { $sum: "$puntosOfrecidos" },
                puntosFTConsumidos: { $sum: "$puntosFTConsumidos" },
                cantidadCompradas: { $sum: "$cantidadCompradas" },
                nombreMarca: { $first: "$dataMarca.nombre" },
                idMarca: { $first: "$dataMarca._id" }
              }
            }
          ],
          categoria: [
            {
              $group: {
                _id: "$dataCat._id",
                puntosOfrecidos: { $first: "$puntosOfrecidos" },
                puntosFTConsumidos: { $sum: "$puntosFTConsumidos" },
                cantidadCompradas: { $sum: "$cantidadCompradas" },
                nombreCat: { $first: "$dataCat.nombre" },
                idCat: { $first: "$dataCat._id" }
              }
            }
          ],
        }
      }
    ]).exec(callback);
  },
};

const Producto = (module.exports = mongoose.model("Producto", ProductoSchema));

module.exports.getProductosSaldoPromocion = function (req, callback) {
  const query = { $or: [{ promocion: true }, { saldos: true }] };
  Producto.find(query).exec(callback);
};

module.exports.getGraficasPromo = function (req, callback) {
  var ObjectId = require("mongoose").Types.ObjectId;
  const query = {
    codigo_distribuidor: new ObjectId(req.trim()),
    promocion: true,
  };
  this.aggregate([
    {
      $match: query,
    },
    {
      $lookup: {
        from: "reportepedidos", //Nombre de la colecccion a relacionar
        localField: "_id", //Nombre del campo de la coleccion actual
        foreignField: "productoId", //Nombre del campo de la coleccion a relacionar
        as: "pedidos_info", //Nombre del campo donde se insertara todos los documentos relacionados
        pipeline: [
          {
            $match: {
              $or: [
                { estaActTraking: "Entregado" },
                { estaActTraking: "Recibido" },
                { estaActTraking: "Calificado" },
              ],
            },
          },
        ],
      },
    },
    {
      $project: {
        _id: "$_id",
        producto: "$nombre",
        unidades_ofrecidas: "$precios.inventario_unidad",
        unidades_compradas: { $sum: "$pedidos_info.unidadesCompradas" },
      },
    },
  ]).exec(callback);
};
module.exports.getGraficasSaldos = function (req, callback) {
  var ObjectId = require("mongoose").Types.ObjectId;
  const query = { codigo_distribuidor: new ObjectId(req.trim()), saldos: true };
  this.aggregate([
    {
      $match: query,
    },
    {
      $lookup: {
        from: "reportepedidos", //Nombre de la colecccion a relacionar
        localField: "_id", //Nombre del campo de la coleccion actual
        foreignField: "productoId", //Nombre del campo de la coleccion a relacionar
        as: "pedidos_info", //Nombre del campo donde se insertara todos los documentos relacionados
        pipeline: [
          {
            $match: {
              $or: [
                { estaActTraking: "Entregado" },
                { estaActTraking: "Recibido" },
                { estaActTraking: "Calificado" },
              ],
            },
          },
        ],
      },
    },
    {
      $project: {
        _id: "$_id",
        producto: "$nombre",
        unidades_ofrecidas: "$precios.inventario_unidad",
        unidades_compradas: { $sum: "$pedidos_info.unidadesCompradas" },
      },
    },
  ]).exec(callback);
};
module.exports.getProductosAceptados = function (callback) {
  const query = {
    $or: [
      { estadoActualizacion: "Aceptado" },
      { estadoActualizacion: "Inactivo" },
      { estadoActualizacion: "Administrador" },
    ],
  };
  this.aggregate([
    {
      $match: query,
    },
    {
      $lookup: {
        from: marcaBD.collection.name, //Nombre de la colecccion a relacionar
        localField: "marca_producto", //Nombre del campo de la coleccion actual
        foreignField: "_id", //Nombre del campo de la coleccion a relacionar
        as: "marca_productos_query", //Nombre del campo donde se insertara todos los documentos relacionados
      },
    },
    {
      $lookup: {
        from: organizacionBD.collection.name, //Nombre de la colecccion a relacionar
        localField: "codigo_organizacion", //Nombre del campo de la coleccion actual
        foreignField: "_id", //Nombre del campo de la coleccion a relacionar
        as: "organizacion_query", //Nombre del campo donde se insertara todos los documentos relacionados
      },
    },
    {
      $lookup: {
        from: lineaBD.collection.name, //Nombre de la colecccion a relacionar
        localField: "linea_producto", //Nombre del campo de la coleccion actual
        foreignField: "_id", //Nombre del campo de la coleccion a relacionar
        as: "linea_productos_query", //Nombre del campo donde se insertara todos los documentos relacionados
      },
    },
    {
      $lookup: {
        from: "distribuidors", //Nombre de la colecccion a relacionar
        localField: "codigo_distribuidor", //Nombre del campo de la coleccion actual
        foreignField: "_id", //Nombre del campo de la coleccion a relacionar
        as: "dataDistribuidor", //Nombre del campo donde se insertara todos los documentos relacionados
      },
    },
    {
      $lookup: {
        from: categoriaBD.collection.name, //Nombre de la colecccion a relacionar
        localField: "categoria_producto", //Nombre del campo de la coleccion actual
        foreignField: "_id", //Nombre del campo de la coleccion a relacionar
        as: "cat_productos_query", //Nombre del campo donde se insertara todos los documentos relacionados
      },
    },
    {
      $project: {
        _id: "$_id",
        estado: "$estadoActualizacion",
        codigoFeat: "$codigo_ft",
        codigoOrganizacion: "$codigo_organizacion_producto",
        codigoDistribuidor: "$codigo_distribuidor",
        codigoDistribuidor_producto: "$codigo_distribuidor_producto",
        fotos: "$fotos",
        nombreProducto: "$nombre",
        nombreDistribuidor: "$dataDistribuidor.nombre",
        unidadMedida: "$precios.unidad_medida",
        cantMedida: "$precios.cantidad_medida",
        presentacionCaja: "$precios.und_x_caja",
        precioCaja: "$precios.precio_caja",
        precioUnidad: "$precios.precio_unidad",
        marca: "$marca_productos_query.nombre",
        categoria: "$cat_productos_query.nombre",
        linea: "$linea_productos_query.nombre",
        organizacion: "$organizacion_query.nombre",
        descripcion: "$descripcion",
        fechaAceptado: "$fechaAceptado",
      },
    },
    {
      $sort: { fechaAceptado: -1 },
    },
  ]).exec(callback);
};
module.exports.productosListaAceptados = function (callback) {
  const query = { estadoActualizacion: "Aceptado" };
  this.aggregate([
    {
      $match: query,
    },
    {
      $lookup: {
        from: marcaBD.collection.name, //Nombre de la colecccion a relacionar
        localField: "marca_producto", //Nombre del campo de la coleccion actual
        foreignField: "_id", //Nombre del campo de la coleccion a relacionar
        as: "marca_productos_query", //Nombre del campo donde se insertara todos los documentos relacionados
      },
    },
    {
      $lookup: {
        from: organizacionBD.collection.name, //Nombre de la colecccion a relacionar
        localField: "codigo_organizacion", //Nombre del campo de la coleccion actual
        foreignField: "_id", //Nombre del campo de la coleccion a relacionar
        as: "organizacion_query", //Nombre del campo donde se insertara todos los documentos relacionados
      },
    },
    {
      $lookup: {
        from: "distribuidors", //Nombre de la colecccion a relacionar
        localField: "codigo_distribuidor", //Nombre del campo de la coleccion actual
        foreignField: "_id", //Nombre del campo de la coleccion a relacionar
        as: "distribuidor_query", //Nombre del campo donde se insertara todos los documentos relacionados
      },
    },
    {
      $lookup: {
        from: categoriaBD.collection.name, //Nombre de la colecccion a relacionar
        localField: "categoria_producto", //Nombre del campo de la coleccion actual
        foreignField: "_id", //Nombre del campo de la coleccion a relacionar
        as: "cat_productos_query", //Nombre del campo donde se insertara todos los documentos relacionados
      },
    },
    {
      $lookup: {
        from: lineaBD.collection.name, //Nombre de la colecccion a relacionar
        localField: "linea_producto", //Nombre del campo de la coleccion actual
        foreignField: "_id", //Nombre del campo de la coleccion a relacionar
        as: "linea_productos_query", //Nombre del campo donde se insertara todos los documentos relacionados
      },
    },
    {
      $project: {
        _id: "$_id",
        estado: "$estadoActualizacion",
        codigoFeat: "$codigo_ft",
        codigoOrganizacion: "$codigo_organizacion_producto",
        codigoDistribuidor: "$codigo_distribuidor_producto",
        nombreDistribuidor: "$distribuidor_query.nombre",
        nitDistribuidor: "$distribuidor_query.nit_cc",
        paisDistribuidor: "$distribuidor_query.pais",
        departamentoDistribuidor: "$distribuidor_query.departamento",
        ciudadDistribuidor: "$distribuidor_query.ciudad",
        fotos: "$fotos",
        nombreProducto: "$nombre",
        unidadMedida: "$precios.unidad_medida",
        cantMedida: "$precios.cantidad_medida",
        presentacionCaja: "$precios.und_x_caja",
        precioCaja: "$precios.precio_caja",
        precioUnidad: "$precios.precio_unidad",
        marca: "$marca_productos_query.nombre",
        categoria: "$cat_productos_query.nombre",
        linea: "$linea_productos_query.nombre",
        organizacion: "$organizacion_query.nombre",
        descripcion: "$descripcion",
        fechaAceptado: "$fechaAceptado",
      },
    },
    {
      $sort: { fechaAceptado: -1 },
    },
  ]).exec(callback);
};
module.exports.getAllProductos = function (callback) {
  const query = {
    $or: [
      { estadoActualizacion: "Aceptado" },
      { estadoActualizacion: "Administrador" },
      { estadoActualizacion: "Inactivo" },
    ],
  };
  this.aggregate([
    {
      $match: query,
    },
    {
      $group: {
        _id: "$_id",
        estado: { $first: "$estadoActualizacion" },
        codigoFeat: { $first: "$codigo_ft" },
        codigoOrganizacion: { $first: "$codigo_organizacion" },
        codigoDistribuidor: { $first: "$codigo_distribuidor" },
        codigoDistribuidorProducto: { $first: "$codigo_distribuidor_producto" },
        codigoOrganizacionProducto: { $first: "$codigo_organizacion_producto" },
        descripcion: { $first: "$descripcion" },
        saldos: { $first: "$saldos" },
        fotos: { $first: "$fotos" },
        categoria: { $first: "$categoria_producto" },
        marca: { $first: "$marca_producto" },
        linea: { $first: "$linea_producto" },
        unidadMedida: { $first: "$precios" },
        nombreProducto: { $first: "$nombre" },
        precios: { $first: "$precios" },
        position: { $sum: 1 },
      },
    },
    {
      $addFields: {
        buscador: { $concat: ["$codigoFeat", "-", "$nombreProducto"] },
      },
    },
    {
      $sort: { codigoFeat: 1 },
    },
  ]).exec(callback);
};
module.exports.lastCodFeat = function (callback) {
  Producto.find({}).sort({ codigo_ft: 1 }).exec(callback);
};
/** Productos portafolio de una organizacion */
module.exports.getPortafolioOrganizacion = function (req, callback) {
  let ObjectId = require("mongoose").Types.ObjectId;
  const d = new Date();
  const fecha_referencia = d.setMonth(d.getMonth() - 3);

  const query = {
    estadoActualizacion: "Aceptado",
    codigo_organizacion: new ObjectId(req.trim()),
  };
  this.aggregate([
    {
      $match: query,
    },
    {
      $lookup: {
        from: marcaBD.collection.name, //Nombre de la colecccion a relacionar
        localField: "marca_producto", //Nombre del campo de la coleccion actual
        foreignField: "_id", //Nombre del campo de la coleccion a relacionar
        as: "marca_productos_query", //Nombre del campo donde se insertara todos los documentos relacionados
      },
    },
    {
      $lookup: {
        from: lineaBD.collection.name, //Nombre de la colecccion a relacionar
        localField: "linea_producto", //Nombre del campo de la coleccion actual
        foreignField: "_id", //Nombre del campo de la coleccion a relacionar
        as: "linea_productos_query", //Nombre del campo donde se insertara todos los documentos relacionados
      },
    },
    {
      $lookup: {
        from: categoriaBD.collection.name, //Nombre de la colecccion a relacionar
        localField: "categoria_producto", //Nombre del campo de la coleccion actual
        foreignField: "_id", //Nombre del campo de la coleccion a relacionar
        as: "cat_productos_query", //Nombre del campo donde se insertara todos los documentos relacionados
      },
    },
    {
      $lookup: {
        from: organizacionBD.collection.name, //Nombre de la colecccion a relacionar
        localField: "codigo_organizacion", //Nombre del campo de la coleccion actual
        foreignField: "_id", //Nombre del campo de la coleccion a relacionar
        as: "organizacion_query", //Nombre del campo donde se insertara todos los documentos relacionados
      },
    },
    {
      $lookup: {
        from: "distribuidors", //Nombre de la colecccion a relacionar
        localField: "codigo_distribuidor", //Nombre del campo de la coleccion actual
        foreignField: "_id", //Nombre del campo de la coleccion a relacionar
        as: "distribuidor_query", //Nombre del campo donde se insertara todos los documentos relacionados
      },
    },
    {
      $lookup: {
        from: "reportepedidos", //Nombre de la colecccion a relacionar
        localField: "_id", //Nombre del campo de la coleccion actual
        foreignField: "productoId", //Nombre del campo de la coleccion a relacionar
        pipeline: [
          {
            $match: {
              createdAt: {
                $gte: new Date(fecha_referencia),
              },
              $or: [
                { estaActTraking: "Entregado" },
                { estaActTraking: "Recibido" },
                { estaActTraking: "Calificado" },
              ],
            },
          },
          {
            $group: {
              _id: "$idOrganizacion",
              total: { $sum: "$totalCompra" },
            },
          },
        ],
        as: "Cant_compra", //Nombre del campo donde se insertara todos los documentos relacionados
      },
    },
    {
      $lookup: {
        from: "reportepedidos", //Nombre de la colecccion a relacionar
        localField: "_id", //Nombre del campo de la coleccion actual
        foreignField: "productoId", //Nombre del campo de la coleccion a relacionar
        pipeline: [
          {
            $match: {
              createdAt: {
                $gte: new Date(fecha_referencia),
              },
              $or: [
                { estaActTraking: "Entregado" },
                { estaActTraking: "Recibido" },
                { estaActTraking: "Calificado" },
              ],
            },
          },
          {
            $group: {
              _id: "$idPunto",
            },
          },
        ],
        as: "estab_alcanzados", //Nombre del campo donde se insertara todos los documentos relacionados
      },
    },
    {
      $project: {
        _id: "$_id",
        idDistribuidores: "$distribuidor_query._id",
        nombreProducto: "$nombre",
        codigo_organizacion_producto: "$codigo_organizacion_producto",
        organizacion: "$organizacion_query.nombre",
        codigoFeat: "$codigo_ft",
        categoria: "$cat_productos_query.nombre",
        linea: "$linea_productos_query.nombre",
        marca: "$marca_productos_query.nombre",
        descripcion: "$descripcion",
        cantMedida: "$precios.cantidad_medida",
        unidadMedida: "$precios.unidad_medida",
        puntos_ft_unidad: "$precios.puntos_ft_unidad",
        presentacionCaja: "$precios.und_x_caja",
        precioUnidad: "$precios.precio_unidad",
        precioCaja: "$precios.precio_caja",
        ventas_tres_meses: {
          $arrayElemAt: ["$Cant_compra.total", 0],
        },
        estab_alcanzados: {
          $size: "$estab_alcanzados",
        },
      },
    },
    {
      $group: {
        _id: "$codigoFeat",
        organizacion: { $first: { $arrayElemAt: ["$organizacion", 0] } },
        codigo_organizacion_producto: {
          $first: "$codigo_organizacion_producto",
        },
        codigoFeat: { $first: "$codigoFeat" },
        nombre: { $first: "$nombreProducto" },
        categoria: { $first: { $arrayElemAt: ["$categoria", 0] } },
        linea: { $first: { $arrayElemAt: ["$linea", 0] } },
        marca: { $first: { $arrayElemAt: ["$marca", 0] } },
        descripcion: { $first: "$descripcion" },
        unidadMedida: { $first: { $arrayElemAt: ["$unidadMedida", 0] } },
        cantMedida: { $first: { $arrayElemAt: ["$cantMedida", 0] } },
        precio_promedio: { $avg: { $arrayElemAt: ["$precioUnidad", 0] } },
        puntos_ft_unidad: {
          $first: { $arrayElemAt: ["$puntos_ft_unidad", 0] },
        },
        total_distribuidores: { $sum: { $size: "$idDistribuidores" } },
        estab_alcanzados: { $sum: "$estab_alcanzados" },
        ventas_tres_meses: { $sum: "$ventas_tres_meses" },
      },
    },
    {
      $sort: { nombre: -1 },
    },
    /*
{
      $addFields: {
        costoTotalProducto: {
          $count: ["$estab_alcanzados"],
        },
      },
    },




    {
      $lookup: {
        from: "reportepedidos",
        let: {
          first: "$_id",
          second: new ObjectId(req.trim()),
        },
        pipeline: [
          // Retorna solo las compras entre punto y distribuidor
          {
            $match: {
              $expr: {
                $and: [
                  {
                    $eq: ["$productoId", "$$first"],
                  },
                  {
                    $eq: ["$idOrganizacion", "$$second"],
                  },
                ],
              },
            },
          },
          //Data de los pedidos
          {
            $lookup: {
              from: "pedidos",
              localField: "idPedido",
              foreignField: "_id",
              as: "data_pedido",
            },
          },
          {
            $unwind: "$data_pedido",
          },
          {
            $match: {
              "data_pedido.createdAt": {
                $gte: new Date(fecha_referencia),
                $lte: new Date(),
              },
              $or: [
                { "data_pedido.estado": "Entregado" },
                { "data_pedido.estado": "Recibido" },
                { "data_pedido.estado": "Calificado" },
              ],
            },
          },
          {
            $addFields: {
              costoTotalProducto: {
                $multiply: ["$unidadesCompradas", "$costoProductos"],
              },
            },
          },
          {
            $group: {
              _id: "$idOrganizacion",
              total: { $sum: "$costoTotalProducto" },
            },
          },
        ],
        as: "data_compras",
      },
    },
    {
      $lookup: {
        from: "reportepedidos",
        let: {
          first: "$_id",
          second: new ObjectId(req.trim()),
        },
        pipeline: [
          {
            $match: {
              $expr: {
                $and: [
                  {
                    $eq: ["$productoId", "$$first"],
                  },
                  {
                    $eq: ["$idOrganizacion", "$$second"],
                  },
                ],
              },
            },
          },
          {
            $lookup: {
              from: "pedidos",
              localField: "idPedido",
              foreignField: "_id",
              as: "data_pedido",
            },
          },
          {
            $unwind: "$data_pedido",
          },
          {
            $match: {
              "data_pedido.createdAt": {
                $gte: new Date(fecha_referencia),
              },
              $or: [
                { "data_pedido.estado": "Entregado" },
                { "data_pedido.estado": "Recibido" },
                { "data_pedido.estado": "Calificado" },
              ],
            },
          },
          {
            $group: {
              _id: "$idPunto",
              total: { $sum: 1 },
            },
          },
        ],
        as: "data_estab_alcanzados",
      },
    },
    {
      $project: {
        _id: "$_id",
        idDistribuidores: "$distribuidor_query._id",
        nombreProducto: "$nombre",
        codigo_organizacion_producto: "$codigo_organizacion_producto",
        organizacion: "$organizacion_query.nombre",
        codigoFeat: "$codigo_ft",
        categoria: "$cat_productos_query.nombre",
        linea: "$linea_productos_query.nombre",
        marca: "$marca_productos_query.nombre",
        descripcion: "$descripcion",
        cantMedida: "$precios.cantidad_medida",
        unidadMedida: "$precios.unidad_medida",
        puntos_ft_unidad: "$precios.puntos_ft_unidad",
        presentacionCaja: "$precios.und_x_caja",
        precioUnidad: "$precios.precio_unidad",
        precioCaja: "$precios.precio_caja",
        ventas_tres_meses: {
          $arrayElemAt: ["$data_compras.total", 0],
        },
        estab_alcanzados: {
          $arrayElemAt: ["$data_estab_alcanzados.total", 0],
        },
      },
    },
    {
      $group: {
        _id: "$codigoFeat",
        organizacion: { $first: { $arrayElemAt: ["$organizacion", 0] } },
        codigo_organizacion_producto: {
          $first: "$codigo_organizacion_producto",
        },
        codigoFeat: { $first: "$codigoFeat" },
        nombre: { $first: "$nombreProducto" },
        categoria: { $first: { $arrayElemAt: ["$categoria", 0] } },
        linea: { $first: { $arrayElemAt: ["$linea", 0] } },
        marca: { $first: { $arrayElemAt: ["$marca", 0] } },
        descripcion: { $first: "$descripcion" },
        unidadMedida: { $first: { $arrayElemAt: ["$unidadMedida", 0] } },
        cantMedida: { $first: { $arrayElemAt: ["$cantMedida", 0] } },
        precio_promedio: { $avg: { $arrayElemAt: ["$precioUnidad", 0] } },
        puntos_ft_unidad: {
          $first: { $arrayElemAt: ["$puntos_ft_unidad", 0] },
        },
        total_distribuidores: { $sum: { $size: "$idDistribuidores" } },
        estab_alcanzados: { $sum: "$estab_alcanzados" },
        ventas_tres_meses: { $sum: "$ventas_tres_meses" },
      },
    },
    {
      $sort: { nombre: -1 },
    },*/
  ]).exec(callback);
};
/** Productos portafolio de una organizacion */
module.exports.getProductosPorOrganizacionDistribuidor = function (
  req,
  callback
) {
  let ObjectId = require("mongoose").Types.ObjectId;
  const query = {
    estadoActualizacion: "Aceptado",
    codigo_organizacion: new ObjectId(req.idOrg.trim()),
  };
  this.aggregate([
    {
      $match: query,
    },
    {
      $lookup: {
        from: "distribuidors", //Nombre de la colecccion a relacionar
        localField: "codigo_distribuidor", //Nombre del campo de la coleccion actual
        foreignField: "_id", //Nombre del campo de la coleccion a relacionar
        as: "data_distribuidor", //Nombre del campo donde se insertara todos los documentos relacionados
      },
    },
    {
      $project: {
        producto_id: "$_id",
        distribuidor_nombre: "$data_distribuidor.nombre",
        producto_nombre: "$nombre",
      },
    },
    {
      $group: {
        _id: "$distribuidor_nombre",
        distribuidor_nombre: {
          $first: { $arrayElemAt: ["$distribuidor_nombre", 0] },
        },
        total_productos: { $sum: 1 },
      },
    },
    {
      $sort: { total_productos: -1 },
    },
  ]).exec(callback);
};
module.exports.getInformeDistribuidorTablaSaldosPromos = function (
  req,
  callback
) {
  let ObjectId = require("mongoose").Types.ObjectId;
  this.aggregate([
    {
      $match: {
        codigo_distribuidor: new ObjectId(req.idDistribuidor),
        $or: [{ promocion: true }, { saldos: true }],
      },
    },
    {
      $lookup: {
        from: "reportepedidos",
        localField: "_id", //Nombre del campo de la coleccion actual
        foreignField: "productoId", //Nombre del campo de la coleccion a relacionar
        pipeline: [
          {
            $facet: {
              pedidos_numero: [
                {
                  $group: {
                    _id: "$productoId",
                    total: { $sum: "$unidadesCompradas" },
                  },
                },
              ],
              pedidos_valor: [
                {
                  $addFields: {
                    costoTotalProducto: {
                      $multiply: ["$unidadesCompradas", "$costoProductos"],
                    },
                  },
                },
                {
                  $group: {
                    _id: "$idPunto",
                    total: { $sum: "$costoTotalProducto" },
                  },
                },
              ],
              pedidos_puntos_alcanzados: [
                {
                  $group: {
                    _id: "$idPedido",
                    idPunto: { $first: "$idPunto" },
                  },
                },
                {
                  $group: {
                    _id: "$idPunto",
                    total: { $sum: 1 },
                  },
                },
              ],
              pedidos_sillas_alcanzados: [
                {
                  $group: {
                    _id: "$idPedido",
                    puntoSillas: { $first: "$puntoSillas" },
                  },
                },
                {
                  $group: {
                    _id: "$puntoSillas",
                    total: { $sum: "$puntoSillas" },
                  },
                },
              ],
            },
          },
        ],
        as: "data_compras",
      },
    },
    {
      $addFields: {
        pedidos_numero: {
          $arrayElemAt: ["$data_compras.pedidos_numero.total", 0],
        },
        pedidos_valor: {
          $arrayElemAt: ["$data_compras.pedidos_valor.total", 0],
        },
        pedidos_puntos_alcanzados: {
          $arrayElemAt: ["$data_compras.pedidos_puntos_alcanzados.total", 0],
        },
        pedidos_sillas_alcanzados: {
          $arrayElemAt: ["$data_compras.pedidos_sillas_alcanzados.total", 0],
        },
      },
    },
    {
      $project: {
        _id: "$_id",
        estado: "$estadoActualizacion",
        saldos: "$saldos",
        promocion: "$promocion",
        codigo_saldo_promo: "$codigo_promo",
        descripcion: "$descripcion",
        codigo_distribuidor_producto: "$codigo_distribuidor_producto",
        nombre: "$nombre",
        inventario_unidad: {
          $arrayElemAt: ["$precios.inventario_unidad", 0],
        },
        precio_unidad: {
          $arrayElemAt: ["$precios.precio_unidad", 0],
        },
        precio_descuento: {
          $arrayElemAt: ["$precios.precio_descuento", 0],
        },
        pedidos_numero: {
          $arrayElemAt: ["$pedidos_numero", 0],
        },
        pedidos_valor: {
          $arrayElemAt: ["$pedidos_valor", 0],
        },
        pedidos_puntos_alcanzados: {
          $arrayElemAt: ["$pedidos_puntos_alcanzados", 0],
        },
        pedidos_sillas_alcanzados: {
          $arrayElemAt: ["$pedidos_sillas_alcanzados", 0],
        },
        fecha_inicio: "$createdAt",
        fecha_vencimiento: "$fecha_vencimiento",
      },
    },
  ]).exec(callback);
};
/** Grafica sadlos distribuidor */
module.exports.getInformeDistribuidorGraficoSaldos = function (req, callback) {
  let ObjectId = require("mongoose").Types.ObjectId;
  this.aggregate([
    {
      $match: {
        codigo_distribuidor: new ObjectId(req.idDistribuidor),
        saldos: true,
      },
    },
    {
      $facet: {
        ofrecidos: [
          {
            $match: {
              estadoActualizacion: "Aceptado",
            },
          },
          {
            $project: {
              _id: "$_id",
              codigo_distribuidor: "$codigo_distribuidor",
              ofrecidos: {
                $arrayElemAt: ["$precios.inventario_unidad", 0],
              },
            },
          },
          {
            $group: {
              _id: "$codigo_distribuidor",
              ofrecidos: { $sum: "$ofrecidos" },
            },
          },
        ],
        vendidos: [
          {
            $lookup: {
              from: "reportepedidos",
              localField: "_id", //Nombre del campo de la coleccion actual
              foreignField: "productoId", //Nombre del campo de la coleccion a relacionar
              pipeline: [
                {
                  $project: {
                    unidadesCompradas: "$unidadesCompradas",
                  },
                },
              ],
              as: "data_compras",
            },
          },
          {
            $unwind: "$data_compras",
          },
          {
            $group: {
              _id: "$codigo_distribuidor",
              vendidos: { $sum: "$data_compras.unidadesCompradas" },
            },
          },
        ],
      },
    },
    {
      $project: {
        ofrecidos: {
          $arrayElemAt: ["$ofrecidos.ofrecidos", 0],
        },
        vendidos: {
          $arrayElemAt: ["$vendidos.vendidos", 0],
        },
      },
    },
  ]).exec(callback);
};
/** Grafica promocion distribuidor */
module.exports.getInformeDistribuidorGraficoPromocion = function (
  req,
  callback
) {
  let ObjectId = require("mongoose").Types.ObjectId;
  this.aggregate([
    {
      $match: {
        codigo_distribuidor: new ObjectId(req.idDistribuidor),
        promocion: true,
      },
    },
    {
      $facet: {
        ofrecidos: [
          {
            $match: {
              estadoActualizacion: "Aceptado",
            },
          },
          {
            $project: {
              _id: "$_id",
              codigo_distribuidor: "$codigo_distribuidor",
              ofrecidos: {
                $arrayElemAt: ["$precios.inventario_unidad", 0],
              },
            },
          },
          {
            $group: {
              _id: "$codigo_distribuidor",
              ofrecidos: { $sum: "$ofrecidos" },
            },
          },
        ],
        vendidos: [
          {
            $lookup: {
              from: "reportepedidos",
              localField: "_id", //Nombre del campo de la coleccion actual
              foreignField: "productoId", //Nombre del campo de la coleccion a relacionar
              pipeline: [
                {
                  $project: {
                    unidadesCompradas: "$unidadesCompradas",
                  },
                },
              ],
              as: "data_compras",
            },
          },
          {
            $unwind: "$data_compras",
          },
          {
            $group: {
              _id: "$codigo_distribuidor",
              vendidos: { $sum: "$data_compras.unidadesCompradas" },
            },
          },
        ],
      },
    },
    {
      $project: {
        ofrecidos: {
          $arrayElemAt: ["$ofrecidos.ofrecidos", 0],
        },
        vendidos: {
          $arrayElemAt: ["$vendidos.vendidos", 0],
        },
      },
    },
  ]).exec(callback);
};
module.exports.productosAcumCat = function (query, callback) {
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
      $lookup: {
        from: "categoria_productos", //Nombre de la colecccion a relacionar
        localField: "categoria_producto", //Nombre del campo de la coleccion actual
        foreignField: "_id", //Nombre del campo de la coleccion a relacionar
        as: "categoria_productos_query", //Nombre del campo donde se insertara todos los documentos relacionados
      },
    },
    {
      $project: {
        label: "$categoria_productos_query.nombre",
        total: { $sum: 1 },
      },
    },
    {
      $group: {
        _id: "$label",
        cantidad: { $sum: "$total" },
      },
    },
    {
      $sort: { cantidad: -1 },
    },
  ]).exec(callback);
};

module.exports.categoriasDistribuidorGraficas = function (query, callback) {
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
      $lookup: {
        from: "categoria_productos", //Nombre de la colecccion a relacionar
        localField: "categoria_producto", //Nombre del campo de la coleccion actual
        foreignField: "_id", //Nombre del campo de la coleccion a relacionar
        as: "categoria_productos_query", //Nombre del campo donde se insertara todos los documentos relacionados
      },
    },
    {
      $project: {
        label: "$categoria_productos_query.nombre",
        distribuidor: "$codigo_distribuidor",
      },
    },
    {
      $group: {
        _id: "$label",
        cantidad: { $sum: 1 },
      },
    },
  ]).exec(callback);
};
// Detalle producto de una organización con datos promedios de el producto
module.exports.getDetallePromProdByOrg = function (query, callback) {
  let ObjectId = require("mongoose").Types.ObjectId;
  this.aggregate([
    {
      $match: {
        _id: new ObjectId(query.id_producto),
        codigo_organizacion: new ObjectId(query.id_organizacion),
        estadoActualizacion: "Aceptado",
        promocion: false,
        saldos: false,
      },
    },
    {
      $project: {
        _id: "$_id",
        nombre: "$nombre",
        descripcion: "$descripcion",
        fotos: "$fotos",
        codigo_ft: "$codigo_ft",
        codigo_organizacion: "$codigo_organizacion",
        codigo_organizacion_producto: "$codigo_organizacion_producto",
        marca_producto: "$marca_producto",
        linea_producto: "$linea_producto",
        categoria_producto: "$categoria_producto",
        precios: "$precios",
        inventario_unidad: {
          $arrayElemAt: ["$precios.inventario_unidad", 0],
        },
        inventario_caja: {
          $arrayElemAt: ["$precios.inventario_caja", 0],
        },
        precio_unidad: {
          $arrayElemAt: ["$precios.precio_unidad", 0],
        },
        precio_caja: {
          $arrayElemAt: ["$precios.precio_caja", 0],
        },
        puntos_ft_caja: {
          $arrayElemAt: ["$precios.puntos_ft_caja", 0],
        },
        puntos_ft_unidad: {
          $arrayElemAt: ["$precios.puntos_ft_unidad", 0],
        },
        und_x_caja: {
          $arrayElemAt: ["$precios.und_x_caja", 0],
        },
        cantidad_medida: {
          $arrayElemAt: ["$precios.cantidad_medida", 0],
        },
        unidad_medida: {
          $arrayElemAt: ["$precios.unidad_medida", 0],
        },
      },
    },
    {
      $lookup: {
        from: marcaBD.collection.name, //Nombre de la colecccion a relacionar
        localField: "marca_producto", //Nombre del campo de la coleccion actual
        foreignField: "_id", //Nombre del campo de la coleccion a relacionar
        pipeline: [
          {
            $project: {
              marca_producto: "$nombre",
            },
          },
        ],
        as: "data_marca_producto", //Nombre del campo donde se insertara todos los documentos relacionados
      },
    },
    {
      $lookup: {
        from: lineaBD.collection.name, //Nombre de la colecccion a relacionar
        localField: "linea_producto", //Nombre del campo de la coleccion actual
        foreignField: "_id", //Nombre del campo de la coleccion a relacionar
        pipeline: [
          {
            $project: {
              linea_producto: "$nombre",
            },
          },
        ],
        as: "data_linea_producto", //Nombre del campo donde se insertara todos los documentos relacionados
      },
    },
    {
      $lookup: {
        from: categoriaBD.collection.name, //Nombre de la colecccion a relacionar
        localField: "categoria_producto", //Nombre del campo de la coleccion actual
        foreignField: "_id", //Nombre del campo de la coleccion a relacionar
        pipeline: [
          {
            $project: {
              categoria_producto: "$nombre",
            },
          },
        ],
        as: "data_categoria_producto", //Nombre del campo donde se insertara todos los documentos relacionados
      },
    },
    {
      $group: {
        _id: "$_id",
        nombre: { $first: "$nombre" },
        codigo_organizacion_producto: {
          $first: "$codigo_organizacion_producto",
        },
        codigo_organizacion: { $first: "$codigo_organizacion" },
        codigo_ft: { $first: "$codigo_ft" },
        descripcion: { $first: "$descripcion" },
        fotos: { $first: "$fotos" },
        categoria_producto: {
          $first: {
            $arrayElemAt: ["$data_categoria_producto.categoria_producto", 0],
          },
        },
        linea_producto: {
          $first: {
            $arrayElemAt: ["$data_linea_producto.linea_producto", 0],
          },
        },
        marca_producto: {
          $first: {
            $arrayElemAt: ["$data_marca_producto.marca_producto", 0],
          },
        },
        inventario_unidad: { $avg: "$inventario_unidad" },
        inventario_caja: { $avg: "$inventario_caja" },
        precio_unidad: { $avg: "$precio_unidad" },
        precio_caja: { $avg: "$precio_caja" },
        puntos_ft_caja: { $avg: "$puntos_ft_caja" },
        puntos_ft_unidad: { $avg: "$puntos_ft_unidad" },
        und_x_caja: { $first: "$und_x_caja" },
        cantidad_medida: { $first: "$cantidad_medida" },
        unidad_medida: { $first: "$unidad_medida" },
      },
    },
  ]).exec(callback);
};
module.exports.detalleProductoCompleto = function (query, callback) {
  let ObjectId = require("mongoose").Types.ObjectId;
  this.aggregate([
    {
      $match: {
        _id: new ObjectId(query._id),
      },
    },
    {
      $lookup: {
        from: organizacionBD.collection.name, //Nombre de la colecccion a relacionar
        localField: "codigo_organizacion", //Nombre del campo de la coleccion actual
        foreignField: "_id", //Nombre del campo de la coleccion a relacionar
        as: "organizacion_query", //Nombre del campo donde se insertara todos los documentos relacionados
      },
    },
    {
      $lookup: {
        from: marcaBD.collection.name, //Nombre de la colecccion a relacionar
        localField: "marca_producto", //Nombre del campo de la coleccion actual
        foreignField: "_id", //Nombre del campo de la coleccion a relacionar
        as: "marca_productos_query", //Nombre del campo donde se insertara todos los documentos relacionados
      },
    },
    {
      $lookup: {
        from: lineaBD.collection.name, //Nombre de la colecccion a relacionar
        localField: "linea_producto", //Nombre del campo de la coleccion actual
        foreignField: "_id", //Nombre del campo de la coleccion a relacionar
        as: "linea_productos_query", //Nombre del campo donde se insertara todos los documentos relacionados
      },
    },
    {
      $lookup: {
        from: categoriaBD.collection.name, //Nombre de la colecccion a relacionar
        localField: "categoria_producto", //Nombre del campo de la coleccion actual
        foreignField: "_id", //Nombre del campo de la coleccion a relacionar
        as: "cat_productos_query", //Nombre del campo donde se insertara todos los documentos relacionados
      },
    },
    {
      $lookup: {
        from: "distribuidors", //Nombre de la colecccion a relacionar
        localField: "codigo_distribuidor", //Nombre del campo de la coleccion actual
        foreignField: "_id", //Nombre del campo de la coleccion a relacionar
        as: "dataDistribuidor", //Nombre del campo donde se insertara todos los documentos relacionados
      },
    },

    {
      $project: {
        _id: "$_id",
        producto_estado: "$estadoActualizacion",
        producto_codigoFeat: "$codigo_ft",
        producto_ficha_tecnica: "$ficha_tecnica",
        producto_cod_distribuidor: "$codigo_distribuidor_producto",
        producto_cod_organizacion: "$codigo_distribuidor_producto",
        producto_fotos: "$fotos",
        producto_nombre: "$nombre",
        producto_unidad_medida: { $arrayElemAt: ["$precios.unidad_medida", 0] },
        producto_cant_medida: { $arrayElemAt: ["$precios.cantidad_medida", 0] },
        producto_und_x_caja: { $arrayElemAt: ["$precios.und_x_caja", 0] },
        producto_precio_caja: { $arrayElemAt: ["$precios.precio_caja", 0] },
        producto_precio_unidad: { $arrayElemAt: ["$precios.precio_unidad", 0] },
        producto_cantidad_medida: {
          $arrayElemAt: ["$precios.cantidad_medida", 0],
        },
        producto_puntosFt_und: {
          $arrayElemAt: ["$precios.puntos_ft_unidad", 0],
        },
        producto_descripcion: "$descripcion",
        producto_fecha_aceptado: "$fechaAceptado",
        producto_fecha_cierre_puntosft: "$fecha_cierre_puntosft",
        producto_fecha_apertura_puntosft: "$fecha_apertura_puntosft",
        codigo_organizacion: "$codigo_organizacion_producto",
        codigo_distribuidor: "$codigo_distribuidor",
        organizacion: { $arrayElemAt: ["$organizacion_query.nombre", 0] },
        distribuidor: { $arrayElemAt: ["$dataDistribuidor.nombre", 0] },
        marca: { $arrayElemAt: ["$marca_productos_query.nombre", 0] },
        categoria: { $arrayElemAt: ["$cat_productos_query.nombre", 0] },
        linea: { $arrayElemAt: ["$linea_productos_query.nombre", 0] },
      },
    },
  ]).exec(callback);
};
module.exports.getDistribudorByProduct = function (query, callback) {
  let ObjectId = require("mongoose").Types.ObjectId;
  this.aggregate([
    {
      $match: {
        codigo_ft: query,
        estadoActualizacion: "Aceptado",
      },
    },
    {
      $group: {
        _id: "$codigo_distribuidor",
        codigo_distribuidor: { $first: "$codigo_distribuidor" },
        estadoActualizacion: { $first: "$estadoActualizacion" },
        codigo_ft: { $first: "$codigo_ft" },
        nombre: { $first: "$nombre" },
        precios: { $first: "$precios" },
      },
    },
    {
      $lookup: {
        from: "distribuidors", //Nombre de la colecccion a relacionar
        localField: "codigo_distribuidor", //Nombre del campo de la coleccion actual
        foreignField: "_id", //Nombre del campo de la coleccion a relacionar
        as: "dataDistribuidor", //Nombre del campo donde se insertara todos los documentos relacionados
      },
    },
    {
      $project: {
        _id: "$_id",
        producto_estado: "$estadoActualizacion",
        producto_codigoFeat: "$codigo_ft",
        producto_nombre: "$nombre",
        producto_precio_caja: { $arrayElemAt: ["$precios.precio_caja", 0] },
        producto_precio_unidad: { $arrayElemAt: ["$precios.precio_unidad", 0] },
        distribuidor: { $arrayElemAt: ["$dataDistribuidor.nombre", 0] },
      },
    },
    {
      $sort: { producto_precio_unidad: -1 },
    },
  ]).exec(callback);
};
module.exports.featMixPublico = function (req, callback) {

  this.aggregate([
    {
      $match: {
        estadoActualizacion: "Aceptado",
        saldos: false,
        promocion: false,
      },
    },
    {
      $lookup: {
        from: marcaBD.collection.name, //Nombre de la colecccion a relacionar
        localField: "marca_producto", //Nombre del campo de la coleccion actual
        foreignField: "_id", //Nombre del campo de la coleccion a relacionar
        as: "marca_productos_query", //Nombre del campo donde se insertara todos los documentos relacionados
      },
    },
    {
      $lookup: {
        from: lineaBD.collection.name, //Nombre de la colecccion a relacionar
        localField: "linea_producto", //Nombre del campo de la coleccion actual
        foreignField: "_id", //Nombre del campo de la coleccion a relacionar
        as: "linea_productos_query", //Nombre del campo donde se insertara todos los documentos relacionados
      },
    },
    {
      $lookup: {
        from: 'categoria_productos', //Nombre de la colecccion a relacionar
        localField: "categoria_producto", //Nombre del campo de la coleccion actual
        foreignField: "_id", //Nombre del campo de la coleccion a relacionar
        as: "categoria_productos_query", //Nombre del campo donde se insertara todos los documentos relacionados
      },
    },
    {
      $lookup: {
        from: 'productos', //Nombre de la colecccion a relacionar
        localField: "codigo_ft", //Nombre del campo de la coleccion actual
        foreignField: "codigo_ft", //Nombre del campo de la coleccion a relacionar
        as: "listProductos", //Nombre del campo donde se insertara todos los documentos relacionados

        pipeline: [
          {
            $project: {
              precios:
              {
                $arrayElemAt: ["$precios.precio_unidad", 0],
              }
            },
          },
          {
            $sort: { precios: 1 },
          },

        ]
      },
    },
    {
      $addFields: {
        priceMin: { $first: "$listProductos" },
        priceMax: { $last: "$listProductos" },
      },
    },
  ]).exec(callback);
};

module.exports.getProductSaldosPromoApp = function (req, callback) {
  var ObjectId = require("mongoose").Types.ObjectId;
  this.aggregate([
    {
      $match: req.query,
    },
    {
      $sort: { createdAt: -1 }
    },
    {
      $skip: req.skip
    },
    {
      $limit: 10
    },
    {
      $addFields: {
        productos_promocion: {
          $map: {
            input: "$productos_promocion",
            as: "id",
            in: { $toObjectId: "$$id" }
          }
        }
      }
    },
    {
      $addFields: {
        dataCopy: '$productos_promocion'
      }
    },
    {
      $lookup: {
        from: "productos",
        localField: "productos_promocion",
        foreignField: "_id",
        as: "producto_info",
        pipeline: [
          {
            $project: {
              fotos: "$fotos",
              nombre: "$nombre",
              _id: "$_id",
              precios: "$precios",


            }
          },
          {
            $addFields: {
              unidades_promo: 0
            }
          }
        ]
      }
    },
    {
      $lookup: {
        from: marcaBD.collection.name, //Nombre de la colecccion a relacionar
        localField: "marca_producto", //Nombre del campo de la coleccion actual
        foreignField: "_id", //Nombre del campo de la coleccion a relacionar
        as: "data_marca", //Nombre del campo donde se insertara todos los documentos relacionados
      },
    },
    {
      $lookup: {
        from: lineaBD.collection.name, //Nombre de la colecccion a relacionar
        localField: "linea_producto", //Nombre del campo de la coleccion actual
        foreignField: "_id", //Nombre del campo de la coleccion a relacionar
        as: "data_linea", //Nombre del campo donde se insertara todos los documentos relacionados
      },
    },
    {
      $lookup: {
        from: categoriaBD.collection.name, //Nombre de la colecccion a relacionar
        localField: "categoria_producto", //Nombre del campo de la coleccion actual
        foreignField: "_id", //Nombre del campo de la coleccion a relacionar
        as: "data_categoria", //Nombre del campo donde se insertara todos los documentos relacionados
      },
    },
    {
      $sort: { nombre: 1 }
    },
  ]).exec(callback);
};
module.exports.getCategoriasAllLineas = function (req, callback) {
  var ObjectId = require("mongoose").Types.ObjectId;
  const query = {
    codigo_distribuidor: new ObjectId(req),
    estadoActualizacion: "Aceptado",
  };
  this.aggregate([
    {
      $match: query,
    },
    {
      $lookup: {
        from: marcaBD.collection.name, //Nombre de la colecccion a relacionar
        localField: "marca_producto", //Nombre del campo de la coleccion actual
        foreignField: "_id", //Nombre del campo de la coleccion a relacionar
        as: "data_marca", //Nombre del campo donde se insertara todos los documentos relacionados
      },
    },
    {
      $lookup: {
        from: lineaBD.collection.name, //Nombre de la colecccion a relacionar
        localField: "linea_producto", //Nombre del campo de la coleccion actual
        foreignField: "_id", //Nombre del campo de la coleccion a relacionar
        as: "data_linea", //Nombre del campo donde se insertara todos los documentos relacionados
      },
    },
    {
      $lookup: {
        from: categoriaBD.collection.name, //Nombre de la colecccion a relacionar
        localField: "categoria_producto", //Nombre del campo de la coleccion actual
        foreignField: "_id", //Nombre del campo de la coleccion a relacionar
        as: "data_categoria", //Nombre del campo donde se insertara todos los documentos relacionados
      },
    },
    {
      $group: {
        _id: "$categoria_producto",
        nombre_categoria: { $first: "$data_categoria.nombre" },
        logo_categoria: { $first: "$data_categoria.logoOn" },
        Lineas: {
          $push: {
            id_linea: "$data_linea._id",
            nombre_linea: "$data_linea.nombre",
          }

        },
      },
    },

  ]).exec(callback);
};

module.exports.getAllPuntosFT = function (req, callback) {
  this.find(
    {
      fecha_apertura_puntosft: { $exists: true },
      fecha_cierre_puntosft: { $exists: true },
      "precios.puntos_ft_unidad": { $gt: 0 }
    }
  )
    .exec(callback);
};