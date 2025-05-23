const LineaBD = require("./linea_producto_model");
const mongoose = require("mongoose");
const Parametrizacion = require('./parametrizacion_model');

// Schema

const ReportePedSchema = mongoose.Schema({
  idPedido: {
    type: mongoose.Schema.Types.ObjectId,
    ref: "Pedido",
    required: true,
  },
  idOrganizacion: {
    type: mongoose.Schema.Types.ObjectId,
    ref: "Organizacion",
    required: false,
  },
  idDistribuidor: {
    type: mongoose.Schema.Types.ObjectId,
    ref: "Distribuidor",
    required: true,
  },
  idComprador: {
    type: mongoose.Schema.Types.ObjectId,
    ref: "Usuario_horeca",
    required: true,
  },
  idUserHoreca: {
    type: mongoose.Schema.Types.ObjectId,
    ref: "Usuario_horeca",
    required: true,
  },
  idTraking: {
    type: mongoose.Schema.Types.ObjectId,
    ref: "Pedido_tracking",
    required: true,
  },
  idPunto: {
    type: mongoose.Schema.Types.ObjectId,
    ref: "Punto_entrega",
    required: true,
  },
  productoId: {
    type: mongoose.Schema.Types.ObjectId,
    ref: "Producto",
    required: true,
  },
  categoriaProducto: {
    type: mongoose.Schema.Types.ObjectId,
    ref: "Categoria_producto",
    required: false,
  },
  lineaProducto: {
    type: mongoose.Schema.Types.ObjectId,
    ref: "Linea_producto",
    required: false,
  },
  marcaProducto: {
    type: mongoose.Schema.Types.ObjectId,
    ref: "Marca_producto",
    required: false,
  },
  tipoUsuario: { type: String, required: false },
  ciudad: { type: String, required: false },
  direccion: { type: String, required: false },
  estaAntTraking: { type: String, required: false },
  estaActTraking: { type: String, required: false },
  nombreProducto: { type: String, required: false },
  codigoFeatProducto: { type: String, required: false },
  codigo_organizacion_producto: { type: String, required: false },
  codigoDistribuidorProducto: { type: String, required: false },
  caja: { type: String, required: false },
  unidadesCompradas: { type: Number, required: false },
  costoProductos: { type: Number, required: false },
  referencia: { type: String, required: false },
  puntos_ft_unidad: { type: Number, required: false, default: 0 },
  puntos_ft_caja: { type: Number, required: false, default: 0 },
  puntos_ft_ganados: { type: Number, required: false, default: 0 },
  puntoCiudad: { type: String, required: false },
  puntoDomicilio: { type: Boolean, required: false },
  puntoSillas: { type: Number, required: false },
  totalCompra:  { 
    type: Number,
    default: 0,
    set: (v) => typeof v === 'string' ? parseFloat(v.replace(",", ".")) : v,
  },
  subtotalCompra:  { 
    type: Number,
    default: 0,
    set: (v) => typeof v === 'string' ? parseFloat(v.replace(",", ".")) : v,
  },
  descuento: { type: Number, required: false },
  puntosGanados: { type: Number, required: false },
  puntosRedimidos: { type: Number, required: false },
  precio_caja_app: { type: Number, required: false, default: 0 },
  detalleProducto: {},
  precioEspecial: [],
  createdAt: { type: Date, required: false, default: Date.now() },
});

ReportePedSchema.statics = {
  getAll: function (query, callback) {
    this.find(query).exec(callback);
  },
  get: function (query, callback) {
    this.findOne(query).exec(callback);
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
  reporteVentasXtipoNegocio: function (query, callback) {
    let ObjectId = require("mongoose").Types.ObjectId;
    this.aggregate([
      {
        $match: {
          idOrganizacion: new ObjectId(query.idOrg),
          createdAt: {
            $gte: new Date(query.inicio),
            $lte: new Date(query.fin),
          },
          $or: [
            { estaActTraking: "Entregado" },
            { estaActTraking: "Recibido" },
            { estaActTraking: "Calificado" },
          ],
        },
      },
      // Filtra las ventas despues de recibido


      {
        $addFields: {
          costoTotalProducto: {
            $multiply: ["$unidadesCompradas", "$costoProductos"],
          },
        },
      },
      {
        $group: {
          _id: "$tipoUsuario",
          total: { $sum: "$costoTotalProducto" },
          count: { $sum: 1 },

        },
      },
    ]).exec(callback);
  },
  calculoPuntoEntrega_compras: function (query, callback) {
    let ObjectId = require("mongoose").Types.ObjectId;
    this.aggregate([
      {
        $match: {
          idPunto: new ObjectId(query.idPunto),
          $or: [
            { estaActTraking: "Entregado" },
            { estaActTraking: "Recibido" },
            { estaActTraking: "Calificado" },
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
          _id: {
            idPedido: "$idPedido",
            productoId: "$productoId",
          },
          productoId: { $last: "$productoId" },
          unidades_producto: { $last: "$unidadesCompradas" },
          costo: { $last: "$costoProductos" },
          costoTotalProducto: { $last: "$costoTotalProducto" },
        },
      },
      {
        $addFields: {
          agrupado: "0001",
        },
      },
      {
        $facet: {
          sumatoria: [
            {
              $group: {
                _id: "$agrupado",
                total: { $sum: "$costoTotalProducto" },
              },
            },
          ],
          productos: [
            {
              $group: {
                _id: "$productoId",
              },
            },
          ],
        },
      },
    ]).exec(callback);
  },
  reporteVentasXCiudad: function (query, callback) {
    let ObjectId = require("mongoose").Types.ObjectId;
    this.aggregate([
      {
        $match: {
          idOrganizacion: new ObjectId(query.idOrg),
          createdAt: {
            $gte: new Date(query.inicio),
            $lte: new Date(query.fin),
          },
        },
      },
      // Filtra las ventas despues de recibido
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
          _id: "$ciudad",
          total: { $sum: "$costoTotalProducto" },
        },
      },
      {
        $sort: { _id: -1 },
      },
    ]).exec(callback);
  },
  reporteVentasXsillas: function (query, callback) {
    let ObjectId = require("mongoose").Types.ObjectId;
    this.aggregate([
      {
        $match: {
          idOrganizacion: new ObjectId(query.idOrg),
          createdAt: {
            $gte: new Date(query.inicio),
            $lte: new Date(query.fin),
          },
        },
      },
      // Filtra las ventas despues de recibido
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
        $facet: {
          sillas0_10: [
            {
              $match: {
                puntoSillas: {
                  $gte: 1,
                  $lte: 10,
                },
              },
            },
          ],
          sillas11_40: [
            {
              $match: {
                puntoSillas: {
                  $gte: 11,
                  $lte: 40,
                },
              },
            },
          ],
          sillas41_80: [
            {
              $match: {
                puntoSillas: {
                  $gte: 41,
                  $lte: 80,
                },
              },
            },
          ],
          sillas81_150: [
            {
              $match: {
                puntoSillas: {
                  $gte: 81,
                  $lte: 151,
                },
              },
            },
          ],
          sillas151_300: [
            {
              $match: {
                puntoSillas: {
                  $gte: 151,
                  $lte: 300,
                },
              },
            },
          ],
          sillas301_500: [
            {
              $match: {
                puntoSillas: {
                  $gte: 301,
                  $lte: 500,
                },
              },
            },
          ],
          sillas501: [
            {
              $match: {
                puntoSillas: {
                  $gte: 501,
                },
              },
            },
          ],
        },
      },
      {
        $project: {
          cant_sillas: [
            {
              label: "1-10",
              count: { $sum: "$sillas0_10.costoTotalProducto" },
            },
            {
              label: "11-40",
              count: { $sum: "$sillas11_40.costoTotalProducto" },
            },
            {
              label: "41-80",
              count: { $sum: "$sillas41_80.costoTotalProducto" },
            },
            {
              label: "81-150",
              count: { $sum: "$sillas81_150.costoTotalProducto" },
            },
            {
              label: "151-300",
              count: { $sum: "$sillas151_300.costoTotalProducto" },
            },
            {
              label: "301-500",
              count: { $sum: "$sillas301_500.costoTotalProducto" },
            },
            {
              label: "+500",
              count: { $sum: "$sillas501.costoTotalProducto" },
            },
          ],
        },
      },
    ]).exec(callback);
  },
  reporteVentasXDistribuidor: function (query, callback) {
    let ObjectId = require("mongoose").Types.ObjectId;
    this.aggregate([
      {
        $match: {
          idOrganizacion: new ObjectId(query.idOrg),
          createdAt: {
            $gte: new Date(query.inicio),
            $lte: new Date(query.fin),
          },
        },
      },
      // Filtra las ventas despues de recibido
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
          $or: [
            { "data_pedido.estado": "Entregado" },
            { "data_pedido.estado": "Recibido" },
            { "data_pedido.estado": "Calificado" },
          ],
        },
      },
      /** Data distribuidores */
      {
        $lookup: {
          from: "distribuidors",
          localField: "idDistribuidor",
          foreignField: "_id",
          as: "dataDistribuidor",
        },
      },
      /** Data cosot producto total */
      {
        $addFields: {
          costoTotalProducto: {
            $multiply: ["$unidadesCompradas", "$costoProductos"], // Precio final
          },
        },
      },
      /** Apruba por nombre y sumamos el costo */
      {
        $group: {
          _id: "$dataDistribuidor.nombre",
          total: {
            $sum: "$costoTotalProducto",
          },
        },
      },
      {
        $sort: { total: -1 },
      },
    ]).exec(callback);
  },
  reporteVentasEstablecimientoConDomicilio: function (query, callback) {
    let ObjectId = require("mongoose").Types.ObjectId;
    this.aggregate([
      {
        $match: {
          idOrganizacion: new ObjectId(query.idOrg),
          //puntoDomicilio: true,
          createdAt: {
            $gte: new Date(query.inicio),
            $lte: new Date(query.fin),
          },
        },
      },
      // Filtra las ventas despues de recibido
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
        $facet: {
          con_domicilio: [
            {
              $match: {
                puntoDomicilio: true,
              },
            },
            {
              $group: {
                _id: "$puntoDomicilio",
                total: { $sum: "$costoTotalProducto" },
              },
            },
          ],
          sin_Domicilio: [
            {
              $match: {
                puntoDomicilio: false,
              },
            },
            {
              $group: {
                _id: "$puntoDomicilio",
                total: { $sum: "$costoTotalProducto" },
              },
            },
          ],
        },
      },
    ]).exec(callback);
  },
  reportePedidosXmes: function (query, callback) {
    let ObjectId = require("mongoose").Types.ObjectId;
    this.aggregate([
      {
        $match: {
          idOrganizacion: new ObjectId(query.idOrg),
          createdAt: {
            $gte: new Date(query.inicio),
            $lte: new Date(query.fin),
          },
        },
      },
      // Filtra las ventas despues de recibido
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
          $or: [
            { "data_pedido.estado": "Entregado" },
            { "data_pedido.estado": "Recibido" },
            { "data_pedido.estado": "Calificado" },
          ],
        },
      },
      {
        $addFields: {
          fechaGroup: {
            $dateToString: {
              format: "%Y-%m",
              date: "$createdAt",
            },
          },
        },
      },
      {
        $group: {
          _id: "$idPedido",
          fechaGroup: { $first: "$fechaGroup" },
        },
      },
      {
        $group: {
          _id: "$fechaGroup",
          total: { $sum: 1 },
        },
      },
    ]).exec(callback);
  },
  reporteVentasXmes: function (query, callback) {
    let ObjectId = require("mongoose").Types.ObjectId;
    this.aggregate([
      {
        $match: {
          idOrganizacion: new ObjectId(query.idOrg),
          createdAt: {
            $gte: new Date(query.inicio),
            $lte: new Date(query.fin),
          },
          $or: [
            { estaActTraking: "Entregado" },
            { estaActTraking: "Recibido" },
            { estaActTraking: "Calificado" },
          ],
        },
      },
      // Filtra las ventas despues de recibido
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
        $addFields: {
          fechaGroup: {
            $dateToString: {
              format: "%Y-%m",
              date: "$createdAt",
            },
          },
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
          _id: "$fechaGroup",
          total: { $sum: "$costoTotalProducto" },
        },
      },
    ]).exec(callback);
  },
  reporteReferenciasXmes: function (query, callback) {
    let ObjectId = require("mongoose").Types.ObjectId;
    this.aggregate([
      {
        $match: {
          idOrganizacion: new ObjectId(query.idOrg),
          createdAt: {
            $gte: new Date(query.inicio),
            $lte: new Date(query.fin),
          },
        },
      },
      // Filtra las ventas despues de recibido
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
          $or: [
            { "data_pedido.estado": "Entregado" },
            { "data_pedido.estado": "Recibido" },
            { "data_pedido.estado": "Calificado" },
          ],
        },
      },
      // grega la fecha del pedido
      {
        $addFields: {
          fechaGroup: {
            $dateToString: {
              format: "%Y-%m",
              date: "$data_pedido.createdAt",
            },
          },
        },
      },
      {
        $group: {
          _id: "$fechaGroup",
          total: { $sum: 1 },
        },
      },
      {
        $sort: { _id: 1 },
      },
    ]).exec(callback);
  },
  reporteVentasXcategorias: function (query, callback) {
    let ObjectId = require("mongoose").Types.ObjectId;
    this.aggregate([
      {
        $match: {
          idOrganizacion: new ObjectId(query.idOrg),
          createdAt: {
            $gte: new Date(query.inicio),
            $lte: new Date(query.fin),
          },
        },
      },
      // Filtra las ventas despues de recibido
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
          $or: [
            { "data_pedido.estado": "Entregado" },
            { "data_pedido.estado": "Recibido" },
            { "data_pedido.estado": "Calificado" },
          ],
        },
      },
      /** Recupera la data de categorias */
      {
        $lookup: {
          from: "categoria_productos",
          localField: "categoriaProducto",
          foreignField: "_id",
          as: "dataCategoria",
        },
      },
      {
        $addFields: {
          costoTotalProducto: {
            $multiply: ["$unidadesCompradas", "$costoProductos"],
          },
        },
      },
      /** agrupa la data por nombre y suma el costo total */
      {
        $group: {
          _id: "$dataCategoria.nombre",
          total: { $sum: "$costoTotalProducto" },
        },
      },
      {
        $sort: { total: -1 },
      },
    ]).exec(callback);
  },
  reporteVentasXmarcas: function (query, callback) {
    let ObjectId = require("mongoose").Types.ObjectId;
    this.aggregate([
      {
        $match: {
          idOrganizacion: new ObjectId(query.idOrg),
          createdAt: {
            $gte: new Date(query.inicio),
            $lte: new Date(query.fin),
          },
        },
      },
      // Filtra las ventas despues de recibido
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
          $or: [
            { "data_pedido.estado": "Entregado" },
            { "data_pedido.estado": "Recibido" },
            { "data_pedido.estado": "Calificado" },
          ],
        },
      },
      {
        $lookup: {
          from: "marca_productos",
          localField:
            "marcaProducto" /** Nombre del campo de la coleccion actual */,
          foreignField:
            "_id" /** Nombre del campo de la coleccion a relacionar */,
          as: "dataMarca" /** Nombre del campo donde se insertara todos los documentos relacionados */,
        },
      },
      /** añande el valor del producto total pedido */
      {
        $addFields: {
          costoTotalProducto: {
            $multiply: ["$unidadesCompradas", "$costoProductos"],
          },
        },
      },
      /** agrupa por nombre y suma el costo */
      {
        $group: {
          _id: "$dataMarca.nombre",
          total: { $sum: "$costoTotalProducto" },
        },
      },
      {
        $sort: { total: -1 },
      },
    ]).exec(callback);
  },
  /************* Establecimientos ************/
  reporteClientesPorMes: function (query, callback) {
    let ObjectId = require("mongoose").Types.ObjectId;
    this.aggregate([
      {
        $match: {
          idOrganizacion: new ObjectId(query.idOrg),
          createdAt: {
            $gte: new Date(query.inicio),
            $lte: new Date(query.fin),
          },
          $or: [
            { estaActTraking: "Entregado" },
            { estaActTraking: "Recibido" },
            { estaActTraking: "Calificado" },
          ],
        },
      },
      /** Data de los pedidos */
      {
        $lookup: {
          from: "pedidos",
          localField: "idPedido",
          foreignField: "_id",
          as: "dataPedido",
        },
      },

      {
        $addFields: {
          fechaGroup: {
            $dateToString: {
              format: "%Y-%m",
              date: "$createdAt",
            },
          },
        },
      },
      {
        $group: {
          _id: {
            fechaGroup: "$fechaGroup",
            idPunto: "$idUserHoreca",
          },
        },
      },
      {
        $group: {
          _id: "$_id.fechaGroup",
          total: { $sum: 1 },
        },
      },
      {
        $sort: { _id: 1 },
      },
      /*
      {
        $group: {
          _id: {
            fechaGroup: "$fechaGroup",
            idPunto: "$idPunto",
          },
          total: { $sum: 1 },
        },
      },
      {
        $addFields: {
          fechaGroup: "$_id.fechaGroup",
        },
      },
      {
        $group: {
          _id: "$fechaGroup",
          total: { $sum: 1 },
        },
      },*/
    ]).exec(callback);
  },
  pedidosEstablecimientoPromedioMes: function (query, callback) {
    let ObjectId = require("mongoose").Types.ObjectId;
    this.aggregate([
      {
        $match: {
          idOrganizacion: new ObjectId(query.idOrg),
          createdAt: {
            $gte: new Date(query.inicio),
            $lte: new Date(query.fin),
          },
          $or: [
            { estaActTraking: "Entregado" },
            { estaActTraking: "Recibido" },
            { estaActTraking: "Calificado" },
          ],

        },
      },
      // Filtra las ventas despues de recibido
      {
        $lookup: {
          from: "pedidos",
          localField: "idPedido",
          foreignField: "_id",
          as: "data_pedido",
        },
      },
      {
        $addFields: {
          costoTotalProducto: {
            $multiply: ["$unidadesCompradas", "$costoProductos"],
          },
          fechaGroup: {
            $dateToString: {
              format: "%Y-%m",
              date: "$createdAt",
            },
          },
        },
      },
      {
        $group: {
          _id: "$idPedido",
          total: { $sum: "$costoTotalProducto" },
        },
      },
      {
        $addFields: {

          contador: 1,

        },
      },
      {
        $group: {
          _id: "$contador",
          total: { $sum: "$total" },
          contador: { $sum: "$contador" },
        },
      },
      {
        $project: {
          cant: { $sum: "$contador" },
          aomunt: { $sum: "$total" },
          total: { $divide: [{ $sum: "$total" }, { $sum: "$contador" }] }
        }
      }

      /*{
        $facet: {
          total_ventas: [
            {
              $group: {
                _id: "$idOrganizacion",
                total: { $sum: "$costoTotalProducto" },
              },
            },
          ],
          promedio_pedidos: [
            {
              $group: {
                _id: "$idPedido",
                fechaGroup: { $first: "$fechaGroup" },
              },
            },
            {
              $group: {
                _id: "$fechaGroup",
                total: { $sum: 1 },
              },
            },
          ],
        },
      },
      {
        $project: {
          total_ventas: { $arrayElemAt: ["$total_ventas.total", 0] },
          promedio_pedidos: { $arrayElemAt: ["$promedio_pedidos.total", 0] },
        },
      },
      {
        $project: {
          total: {
            $trunc: [
              {
                $divide: ["$total_ventas", "$promedio_pedidos"],
              },
              2,
            ],
          },
        },
      },*/
    ]).exec(callback);
  },
  pedidosEstablecimientoPromedioCantidadMes: function (query, callback) {
    let ObjectId = require("mongoose").Types.ObjectId;
    this.aggregate([
      {
        $match: {
          idOrganizacion: new ObjectId(query.idOrg),
          createdAt: {
            $gte: new Date(query.inicio),
            $lte: new Date(query.fin),
          },
        },
      },
      // Filtra las ventas despues de recibido
      {
        $lookup: {
          from: "pedidos",
          localField: "idPedido",
          foreignField: "_id",
          as: "data_pedido",
        },
      },
      {
        $match: {
          $or: [
            { "data_pedido.estado": "Entregado" },
            { "data_pedido.estado": "Recibido" },
            { "data_pedido.estado": "Calificado" },
          ],
        },
      },
      {
        $addFields: {
          cantidad_base: 1,
        },
      },
      {
        $group: {
          _id: "$idPedido",
          sumPedido: { $sum: "$cantidad_base" },
          idOrganizacion: { $first: "$idOrganizacion" },
        },
      },
      {
        $addFields: {
          couPed2: 1,
        },
      },
      {
        $group: {
          _id: "$couPed2",
          total: { $sum: 1 },
          totalsumPedido: { $sum: '$sumPedido' },
        }
      },
      {
        $project: {
          total: {
            $divide: ["$totalsumPedido", "$total"],
          },
        },
      },
      /*{
        $group: {
          _id: "$idOrganizacion",
          total: { $sum: 1 },
        },
      },*/
    ]).exec(callback);
  },
  reporteEstablecimientosPorNegocio: function (query, callback) {
    let ObjectId = require("mongoose").Types.ObjectId;
    this.aggregate([
      {
        $match: {
          idOrganizacion: new ObjectId(query.idOrg),
          createdAt: {
            $gte: new Date(query.inicio),
            $lte: new Date(query.fin),
          },
        },
      },
      {
        $match: {
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
          tipoUsuario: { $first: "$tipoUsuario" },
        },
      },
      {
        $group: {
          _id: "$tipoUsuario",
          total: { $sum: 1 },
        },
      },
    ]).exec(callback);
  },
  reporteEstablecimientosPorSillas: function (query, callback) {
    let ObjectId = require("mongoose").Types.ObjectId;
    this.aggregate([
      {
        $match: {
          idOrganizacion: new ObjectId(query.idOrg),
          createdAt: {
            $gte: new Date(query.inicio),
            $lte: new Date(query.fin),
          },
        },
      },
      {
        $group: {
          _id: {
            idPunto: "$idPunto",
            puntoSillas: "$puntoSillas",
          },
          total: { $sum: 1 },
        },
      },
      {
        $project: {
          idPunto: "$_id.idPunto",
          puntoSillas: "$_id.puntoSillas",
          total: "$total",
        },
      },
      /** Crea nuevas gateogias */
      {
        $facet: {
          sillas11_40: [
            {
              $match: {
                puntoSillas: {
                  $gte: 11,
                  $lte: 40,
                },
              },
            },
          ],
          sillas0_10: [
            {
              $match: {
                puntoSillas: {
                  $gte: 1,
                  $lte: 10,
                },
              },
            },
          ],
          sillas41_80: [
            {
              $match: {
                puntoSillas: {
                  $gte: 41,
                  $lte: 80,
                },
              },
            },
          ],
          sillas151_300: [
            {
              $match: {
                puntoSillas: {
                  $gte: 151,
                  $lte: 300,
                },
              },
            },
          ],
          sillas501: [
            {
              $match: {
                puntoSillas: {
                  $gte: 501,
                },
              },
            },
          ],
          sillas301_500: [
            {
              $match: {
                puntoSillas: {
                  $gte: 301,
                  $lte: 500,
                },
              },
            },
          ],
          sillas81_150: [
            {
              $match: {
                puntoSillas: {
                  $gte: 81,
                  $lte: 151,
                },
              },
            },
          ],
        },
      },
      {
        $project: {
          cant_sillas: [
            {
              label: "1-10",
              count: { $size: "$sillas0_10" },
            },
            {
              label: "11-40",
              count: { $size: "$sillas11_40" },
            },
            {
              label: "41-80",
              count: { $size: "$sillas41_80" },
            },
            {
              label: "81-150",
              count: { $size: "$sillas81_150" },
            },
            {
              label: "151-300",
              count: { $size: "$sillas151_300" },
            },
            {
              label: "301-500",
              count: { $size: "$sillas301_500" },
            },
            {
              label: "+500",
              count: { $size: "$sillas501" },
            },
          ],
        },
      },
    ]).exec(callback);
  },
  reporteEstablecimientosPorCiudad: function (query, callback) {
    let ObjectId = require("mongoose").Types.ObjectId;
    this.aggregate([
      {
        $match: {
          idOrganizacion: new ObjectId(query.idOrg),
          createdAt: {
            $gte: new Date(query.inicio),
            $lte: new Date(query.fin),
          },
        },
      },
      {
        $group: {
          _id: {
            idPunto: "$idPunto",
            ciudad: "$ciudad",
          },
          total: { $sum: 1 },
        },
      },
      {
        $addFields: {
          ciudad: "$_id.ciudad",
        },
      },
      {
        $group: {
          _id: "$ciudad",
          total: { $sum: 1 },
        },
      },
    ]).exec(callback);
  },
  reporteEstablecimientosPorDomicilio: function (query, callback) {
    let ObjectId = require("mongoose").Types.ObjectId;
    this.aggregate([
      {
        $match: {
          idOrganizacion: new ObjectId(query.idOrg),
        },
      },
      /** Depura los puntos repetidos */
      {
        $group: {
          _id: "$idPunto",
          idPunto: { $first: "$idPunto" },
          puntoDomicilio: { $first: "$puntoDomicilio" },
          idPedido: { $last: "$idPedido" },
        },
      },
      {
        $facet: {
          con_domicilio: [
            {
              $match: {
                puntoDomicilio: true,
              },
            },
            {
              $group: {
                _id: "$puntoDomicilio",
                total: { $sum: 1 },
              },
            },
          ],
          sin_Domicilio: [
            {
              $match: {
                puntoDomicilio: false,
              },
            },
            {
              $group: {
                _id: "$puntoDomicilio",
                total: { $sum: 1 },
              },
            },
          ],
        },
      },
    ]).exec(callback);
  },
  reporteEstablecimientosConConvenio: function (query, callback) {
    let ObjectId = require("mongoose").Types.ObjectId;
    this.aggregate([
      {
        $match: {
          idOrganizacion: new ObjectId(query.idOrg),
        },
      },
      /** Depura los puntos duplicados */
      {
        $group: {
          _id: "$idPunto",
          idPedido: { $last: "$idPedido" },
          idPunto: { $first: "$idPunto" },
        },
      },
      /** Valida vinculacion */
      {
        $lookup: {
          from: "distribuidores_vinculados",
          localField: "idPunto",
          foreignField: "punto_entrega",
          as: "dataVinculacion",
        },
      },
      {
        $addFields: {
          dataVinculacion: { $arrayElemAt: ["$dataVinculacion", 0] },
        },
      },
      /** Filtra los aprobados */
      {
        $match: {
          "dataVinculacion.estado": "Aprobado",
        },
      },
      {
        $facet: {
          convenio: [
            {
              $match: {
                "dataVinculacion.convenio": true,
              },
            },
          ],
          noConvenio: [
            {
              $match: {
                "dataVinculacion.convenio": false,
              },
            },
          ],
        },
      },
      {
        $project: {
          convenio: [
            {
              label: "Convenio",
              count: { $size: "$convenio" },
            },
            {
              label: "Sin convenio",
              count: { $size: "$noConvenio" },
            },
          ],
        },
      },
    ]).exec(callback);
  },
  reporteEstablecimientosConCartera: function (query, callback) {
    let ObjectId = require("mongoose").Types.ObjectId;
    this.aggregate([
      {
        $match: {
          idOrganizacion: new ObjectId(query.idOrg),
        },
      },
      /** Depura los punto repetidos */
      {
        $group: {
          _id: "$idPunto",
          idPunto: { $first: "$idPunto" },
          idPedido: { $last: "$idPedido" },
        },
      },
      /** Vinculacion */
      {
        $lookup: {
          from: "distribuidores_vinculados",
          localField: "idPunto",
          foreignField: "punto_entrega",
          as: "dataVinculacion",
        },
      },
      {
        $addFields: {
          dataVinculacion: { $arrayElemAt: ["$dataVinculacion", 0] },
        },
      },
      {
        $match: {
          "dataVinculacion.estado": "Aprobado",
        },
      },

      {
        $facet: {
          cartera: [
            {
              $match: {
                "dataVinculacion.cartera": true,
              },
            },
          ],
          noCartera: [
            {
              $match: {
                "dataVinculacion.cartera": false,
              },
            },
          ],
        },
      },
      {
        $project: {
          cartera: [
            {
              label: "Cartera",
              count: { $size: "$cartera" },
            },
            {
              label: "Sin cartera",
              count: { $size: "$noCartera" },
            },
          ],
        },
      },
    ]).exec(callback);
  },
  reporteEstablecimientosPromProd: function (query, callback) {
    let ObjectId = require("mongoose").Types.ObjectId;
    this.aggregate([
      {
        $match: {
          idOrganizacion: new ObjectId(query.idOrg),
        },
      },
      {
        $group: {
          _id: "$idPedido",
          productos: { $sum: 1 },
        },
      },
      {
        $group: {
          _id: "$productos",
          total: { $sum: 1 },
        },
      },
      {
        $sort: { _id: 1 },
      },
    ]).exec(callback);
  },
  reporteEstablecimientosPorFiguraNegocio: function (query, callback) {
    let ObjectId = require("mongoose").Types.ObjectId;
    this.aggregate([
      {
        $match: {
          idOrganizacion: new ObjectId(query.idOrg),
        },
      },
      /** Depura los punto/pedidos repetidos */
      {
        $group: {
          _id: "$idPunto",
          idPunto: { $first: "$idPunto" },
          idPedido: { $last: "$idPedido" },
        },
      },
      /** Se recupera la info de cada punto */
      {
        $lookup: {
          from: "punto_entregas",
          localField: "idPunto" /** Nombre del campo de la coleccion actual */,
          foreignField:
            "_id" /** Nombre del campo de la coleccion a relacionar */,
          as: "dataPunto" /** Nombre del campo donde se insertara todos los documentos relacionados */,
        },
      },
      {
        $lookup: {
          from: "usuario_horecas",
          localField:
            "dataPunto.usuario_horeca" /** Nombre del campo de la coleccion actual */,
          foreignField:
            "_id" /** Nombre del campo de la coleccion a relacionar */,
          as: "dataHoreca" /** Nombre del campo donde se insertara todos los documentos relacionados */,
        },
      },
      {
        $addFields: {
          dataHoreca: { $arrayElemAt: ["$dataHoreca", 0] },
        },
      },
      {
        $project: {
          idPunto: "$idPunto",
          nombre_establecimiento: "$dataHoreca.nombre_establecimiento",
          tipo_usuario: "$dataHoreca.tipo_usuario",
        },
      },
      {
        $facet: {
          juridica: [
            {
              $match: {
                tipo_usuario: "Jurídica",
              },
            },
          ],
          natural: [
            {
              $match: {
                tipo_usuario: "Natural",
              },
            },
          ],
        },
      },
      {
        $project: {
          tipo_negocio: [
            {
              label: "Jurídica",
              count: { $size: "$juridica" },
            },
            {
              label: "Natural",
              count: { $size: "$natural" },
            },
          ],
        },
      },
    ]).exec(callback);
  },
  reporteEstablecimientosPorTotalPuntos: function (query, callback) {
    let ObjectId = require("mongoose").Types.ObjectId;
    this.aggregate([
      {
        $match: {
          idOrganizacion: new ObjectId(query.idOrg),
        },
      },
      {
        $match: {
          $or: [
            { estaActTraking: "Entregado" },
            { estaActTraking: "Recibido" },
            { estaActTraking: "Calificado" },
          ],
        },
      },
      /** Depura los punto repetidos */
      {
        $group: {
          _id: "$idPunto",
          idPunto: { $first: "$idPunto" },
          idPedido: { $last: "$idPedido" },
        },
      },
      /** Se recupera la info de cada punto */
      {
        $lookup: {
          from: "punto_entregas",
          localField: "idPunto",
          foreignField: "_id",
          pipeline: [
            // {
            //   $project: {
            //     estado: "$estado",
            //   },
            // },
            {
              $match: {
                estado: "Activo",
              },
            },
          ],
          as: "dataPunto",
        },
      },
      {
        $unwind: "$dataPunto",
      },
      {
        $replaceRoot: {
          newRoot: "$dataPunto",
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
        $addFields: {
          dataHoreca: { $arrayElemAt: ["$dataHoreca", 0] },
        },
      },
      {
        $project: {
          idPunto: "$idPunto",
          nombre_establecimiento: "$dataHoreca.nombre_establecimiento",
          idHoreca: "$dataHoreca._id",
        },
      },
      {
        $group: {
          _id: "$idHoreca",
          total: { $sum: 1 },
          nombre_establecimiento: { $first: "$nombre_establecimiento" },
        },
      },
      {
        $facet: {
          puntos6_10: [
            {
              $match: {
                total: {
                  $gte: 6,
                  $lte: 10,
                },
              },
            },
          ],
          puntos11_20: [
            {
              $match: {
                total: {
                  $gte: 11,
                  $lte: 20,
                },
              },
            },
          ],
          puntos21: [
            {
              $match: {
                total: {
                  $gte: 21,
                },
              },
            },
          ],
          puntos1: [
            {
              $match: {
                total: 1,
              },
            },
          ],
          puntos2: [
            {
              $match: {
                total: 2,
              },
            },
          ],
          puntos3_5: [
            {
              $match: {
                total: {
                  $gte: 3,
                  $lte: 5,
                },
              },
            },
          ],
        },
      },
      {
        $project: {
          puntos6_10: [
            {
              label: "6-10",
              count: { $size: "$puntos6_10" },
            },
          ],
          puntos11_20: [
            {
              label: "11-20",
              count: { $size: "$puntos11_20" },
            },
          ],
          puntos21: [
            {
              label: "+21",
              count: { $size: "$puntos21" },
            },
          ],
          puntos1: [
            {
              label: "1",
              count: { $size: "$puntos1" },
            },
          ],
          puntos2: [
            {
              label: "2",
              count: { $size: "$puntos2" },
            },
          ],
          puntos3_5: [
            {
              label: "3-5",
              count: { $size: "$puntos3_5" },
            },
          ],
        },
      },
    ]).exec(callback);
  },
  /***************** Sector ****************/
  sectorVentas: function (query, ciudad, callback) {
    let ObjectId = require("mongoose").Types.ObjectId;
    if (ciudad.ciudad === "todos") {
      this.aggregate([
        {
          $match: {
            $or: [
              { estaActTraking: "Entregado" },
              { estaActTraking: "Recibido" },
              { estaActTraking: "Calificado" },
            ],
          },
        },
        /** Arma la data de la grafica */
        {
          $addFields: {
            costoTotalProducto: {
              $multiply: ["$unidadesCompradas", "$costoProductos"],
            },
          },
        },
        {
          $facet: {
            miOrganizacion: [
              {
                $match: {
                  idOrganizacion: new ObjectId(query.idOrg),
                },
              },
            ],
            otrasOrganizaciones: [
              {
                $match: {},
              },
            ],
          },
        },
        {
          $project: {
            /**  */
            otrasOrganizaciones: [
              {
                label: "Otras organizaciones",
                total: { $sum: "$otrasOrganizaciones.costoTotalProducto" },
              },
            ],
            /** */
            miOrganizacion: [
              {
                label: "Organización",
                total: { $sum: "$miOrganizacion.costoTotalProducto" },
              },
            ],
          },
        },
      ]).exec(callback);
    } else {
      this.aggregate([
        {
          $match: {
            $or: [
              { estaActTraking: "Entregado" },
              { estaActTraking: "Recibido" },
              { estaActTraking: "Calificado" },
            ],
          },
        },
        /** Recupera la ciudad del punto y aplica el filtro de ciudad */
        {
          $lookup: {
            from: "punto_entregas",
            localField: "idPunto",
            foreignField: "_id",
            pipeline: [
              {
                $project: {
                  ciudad: "$ciudad",
                },
              },
            ],
            as: "dataPunto",
          },
        },
        {
          $addFields: {
            dataPunto_: { $arrayElemAt: ["$dataPunto", 0] },
          },
        },
        {
          $addFields: {
            punto_ciudad: "$dataPunto_.ciudad",
          },
        },
        {
          $match: {
            punto_ciudad: ciudad.ciudad,
          },
        },
        /** Arma la data de la grafica */
        {
          $addFields: {
            costoTotalProducto: {
              $multiply: ["$unidadesCompradas", "$costoProductos"],
            },
          },
        },
        {
          $facet: {
            miOrganizacion: [
              {
                $match: {
                  idOrganizacion: new ObjectId(query.idOrg),
                },
              },
            ],
            otrasOrganizaciones: [
              {
                $match: {},
              },
            ],
          },
        },
        {
          $project: {
            miOrganizacion: [
              {
                label: "Organización",
                total: { $sum: "$miOrganizacion.costoTotalProducto" },
              },
            ],
            otrasOrganizaciones: [
              {
                label: "Otras organizaciones",
                total: { $sum: "$otrasOrganizaciones.costoTotalProducto" },
              },
            ],
          },
        },
      ]).exec(callback);
    }
  },
  clientesSector: function (query, ciudad, callback) {
    let ObjectId = require("mongoose").Types.ObjectId;
    if (ciudad.ciudad !== "todos") {
      this.aggregate([
        {
          $match: {
            $or: [
              { estaActTraking: "Entregado" },
              { estaActTraking: "Recibido" },
              { estaActTraking: "Calificado" },
            ],
          },
        },
        /** Recupera la ciudad del punto y aplica el filtro de ciudad */
        {
          $lookup: {
            from: "punto_entregas",
            localField: "idPunto", //Nombre del campo de la coleccion actual
            foreignField: "_id", //Nombre del campo de la coleccion a relacionar
            pipeline: [
              {
                $project: {
                  ciudad: "$ciudad",
                },
              },
            ],
            as: "dataPunto", //Nombre del campo donde se insertara todos los documentos relacionados
          },
        },
        {
          $addFields: {
            dataPunto_: { $arrayElemAt: ["$dataPunto", 0] },
          },
        },
        {
          $addFields: {
            punto_ciudad: "$dataPunto_.ciudad",
          },
        },
        {
          $match: {
            punto_ciudad: ciudad.ciudad,
          },
        },
        {
          $group: {
            _id: {
              idOrganizacion: "$idOrganizacion",
              idPunto: "$idPunto",
            },
            total: { $sum: 1 },
            idPunto: { $first: "$idPunto" },
            idOrganizacion: { $first: "$idOrganizacion" },
          },
        },
        /** Arma la data de la grafica */
        {
          $facet: {
            otrasOrganizaciones: [
              {
                $match: {},
              },
            ],
            miOrganizacion: [
              {
                $match: {
                  idOrganizacion: new ObjectId(query.idOrg),
                },
              },
            ],
          },
        },
        {
          $project: {
            miOrganizacion: [
              {
                label: "Organización",
                total: { $size: "$miOrganizacion" },
              },
            ],
            otrasOrganizaciones: [
              {
                label: "Otras organizaciones",
                total: { $size: "$otrasOrganizaciones" },
              },
            ],
          },
        },
      ]).exec(callback);
    } else {
      this.aggregate([
        {
          $match: {
            $or: [
              { estaActTraking: "Entregado" },
              { estaActTraking: "Recibido" },
              { estaActTraking: "Calificado" },
            ],
          },
        },
        {
          $group: {
            _id: {
              idOrganizacion: "$idOrganizacion",
              idPunto: "$idPunto",
            },
            total: { $sum: 1 },
            idPunto: { $first: "$idPunto" },
            idOrganizacion: { $first: "$idOrganizacion" },
          },
        },
        /** Arma la data de la grafica */
        {
          $facet: {
            miOrganizacion: [
              {
                $match: {
                  idOrganizacion: new ObjectId(query.idOrg),
                },
              },
            ],
            otrasOrganizaciones: [
              {
                $match: {},
              },
            ],
          },
        },

        {
          $project: {
            otrasOrganizaciones: [
              {
                label: "Otras organizaciones",
                total: { $size: "$otrasOrganizaciones" },
              },
            ],
            miOrganizacion: [
              {
                label: "Organización",
                total: { $size: "$miOrganizacion" },
              },
            ],
          },
        },
      ]).exec(callback);
    }
  },
  sillasSector: function (query, ciudad, callback) {
    let ObjectId = require("mongoose").Types.ObjectId;
    if (ciudad.ciudad === "todos") {
      this.aggregate([
        {
          $match: {
            $or: [
              { estaActTraking: "Entregado" },
              { estaActTraking: "Recibido" },
              { estaActTraking: "Calificado" },
            ],
          },
        },
        {
          $lookup: {
            from: "punto_entregas",
            localField: "idPunto", //Nombre del campo de la coleccion actual
            foreignField: "_id", //Nombre del campo de la coleccion a relacionar
            pipeline: [
              {
                $project: {
                  sillas: "$sillas",
                  ciudad: "$ciudad",
                },
              },
            ],
            as: "dataPunto", //Nombre del campo donde se insertara todos los documentos relacionados
          },
        },
        {
          $addFields: {
            dataPunto_: { $arrayElemAt: ["$dataPunto", 0] },
          },
        },
        {
          $addFields: {
            punto_ciudad: "$dataPunto_.ciudad",
            punto_sillas: "$dataPunto_.sillas",
          },
        },
        {
          $group: {
            _id: {
              idOrganizacion: "$idOrganizacion",
              idPunto: "$idPunto",
            },
            total: { $sum: "$punto_sillas" },
            idPunto: { $first: "$idPunto" },
            punto_sillas: { $first: "$punto_sillas" },
            idOrganizacion: { $first: "$idOrganizacion" },
          },
        },
        /** Arma la data de la grafica */
        {
          $facet: {
            miOrganizacion: [
              {
                $match: {
                  idOrganizacion: new ObjectId(query.idOrg),
                },
              },
            ],
            otrasOrganizaciones: [
              {
                $match: {},
              },
            ],
          },
        },
        {
          $project: {
            miOrganizacion: [
              {
                label: "Organización",
                total: { $sum: "$miOrganizacion.punto_sillas" },
              },
            ],

            otrasOrganizaciones: [
              {
                label: "Otras organizaciones",
                total: { $sum: "$otrasOrganizaciones.punto_sillas" },
              },
            ],
          },
        },
      ]).exec(callback);
    } else {
      this.aggregate([
        {
          $match: {
            $or: [
              { estaActTraking: "Entregado" },
              { estaActTraking: "Recibido" },
              { estaActTraking: "Calificado" },
            ],
          },
        },
        /** Recupera la ciudad del punto y aplica el filtro de ciudad */
        {
          $lookup: {
            from: "punto_entregas",
            localField: "idPunto", //Nombre del campo de la coleccion actual
            foreignField: "_id", //Nombre del campo de la coleccion a relacionar
            pipeline: [
              {
                $project: {
                  sillas: "$sillas",
                  ciudad: "$ciudad",
                },
              },
            ],
            as: "dataPunto", //Nombre del campo donde se insertara todos los documentos relacionados
          },
        },
        {
          $addFields: {
            dataPunto_: { $arrayElemAt: ["$dataPunto", 0] },
          },
        },
        {
          $addFields: {
            punto_ciudad: "$dataPunto_.ciudad",
            punto_sillas: "$dataPunto_.sillas",
          },
        },
        {
          $match: {
            punto_ciudad: ciudad.ciudad,
          },
        },
        {
          $group: {
            _id: {
              idOrganizacion: "$idOrganizacion",
              idPunto: "$idPunto",
            },
            total: { $sum: "$punto_sillas" },
            idPunto: { $first: "$idPunto" },
            punto_sillas: { $first: "$punto_sillas" },
            idOrganizacion: { $first: "$idOrganizacion" },
          },
        },
        /** Arma la data de la grafica */
        {
          $facet: {
            otrasOrganizaciones: [
              {
                $match: {},
              },
            ],
            miOrganizacion: [
              {
                $match: {
                  idOrganizacion: new ObjectId(query.idOrg),
                },
              },
            ],
          },
        },
        {
          $project: {
            otrasOrganizaciones: [
              {
                label: "Otras organizaciones",
                total: { $sum: "$otrasOrganizaciones.punto_sillas" },
              },
            ],
            miOrganizacion: [
              {
                label: "Organización",
                total: { $sum: "$miOrganizacion.punto_sillas" },
              },
            ],
          },
        },
      ]).exec(callback);
    }
  },
  categoriasSector: function (query, callback) {
    let ObjectId = require("mongoose").Types.ObjectId;
    if (query.idCat === "todos") {
      this.aggregate([
        {
          $match: {
            $or: [
              { estaActTraking: "Entregado" },
              { estaActTraking: "Recibido" },
              { estaActTraking: "Calificado" },
            ],
          },
        },
        /** Arma la data de la grafica */
        {
          $addFields: {
            costoTotalProducto: {
              $multiply: ["$unidadesCompradas", "$costoProductos"],
            },
          },
        },
        /** Arma la data de la grafica */
        {
          $facet: {
            miOrganizacion: [
              {
                $match: {
                  idOrganizacion: new ObjectId(query.idOrg),
                },
              },
            ],
            otrasOrganizaciones: [
              {
                $match: {},
              },
            ],
          },
        },
        {
          $project: {
            miOrganizacion: [
              {
                label: "Organización",
                total: { $sum: "$miOrganizacion.costoTotalProducto" },
              },
            ],
            otrasOrganizaciones: [
              {
                label: "Otras organizaciones",
                total: { $sum: "$otrasOrganizaciones.costoTotalProducto" },
              },
            ],
          },
        },
      ]).exec(callback);
    } else {
      this.aggregate([
        {
          $match: {
            $or: [
              { estaActTraking: "Entregado" },
              { estaActTraking: "Recibido" },
              { estaActTraking: "Calificado" },
            ],
          },
        },
        {
          $match: {
            categoriaProducto: new ObjectId(query.idCat),
          },
        },
        /** Arma la data de la grafica */
        {
          $addFields: {
            costoTotalProducto: {
              $multiply: ["$unidadesCompradas", "$costoProductos"],
            },
          },
        },
        /** Arma la data de la grafica */
        {
          $facet: {
            miOrganizacion: [
              {
                $match: {
                  idOrganizacion: new ObjectId(query.idOrg),
                },
              },
            ],
            otrasOrganizaciones: [
              {
                $match: {},
              },
            ],
          },
        },
        {
          $project: {
            miOrganizacion: [
              {
                label: "Organización",
                total: { $sum: "$miOrganizacion.costoTotalProducto" },
              },
            ],
            otrasOrganizaciones: [
              {
                label: "Otras organizaciones",
                total: { $sum: "$otrasOrganizaciones.costoTotalProducto" },
              },
            ],
          },
        },
      ]).exec(callback);
    }
  },
  lineaSector: function (query, callback) {
    let ObjectId = require("mongoose").Types.ObjectId;
    if (query.idLinea === "todos") {
      this.aggregate([
        {
          $match: {
            $or: [
              { estaActTraking: "Entregado" },
              { estaActTraking: "Recibido" },
              { estaActTraking: "Calificado" },
            ],
          },
        },
        {
          $addFields: {
            linea_producto: {
              $arrayElemAt: ["$detalleProducto.linea_producto", 0],
            },
          },
        },
        /** Arma la data de la grafica */
        {
          $addFields: {
            costoTotalProducto: {
              $multiply: ["$unidadesCompradas", "$costoProductos"],
            },
          },
        },
        /** Arma la data de la grafica */
        {
          $facet: {
            miOrganizacion: [
              {
                $match: {
                  idOrganizacion: new ObjectId(query.idOrg),
                },
              },
            ],
            otrasOrganizaciones: [
              {
                $match: {},
              },
            ],
          },
        },
        {
          $project: {
            miOrganizacion: [
              {
                label: "Organización",
                total: { $sum: "$miOrganizacion.costoTotalProducto" },
              },
            ],
            otrasOrganizaciones: [
              {
                label: "Otras organizaciones",
                total: { $sum: "$otrasOrganizaciones.costoTotalProducto" },
              },
            ],
          },
        },
      ]).exec(callback);
    } else {
      this.aggregate([
        {
          $match: {
            $or: [
              { estaActTraking: "Entregado" },
              { estaActTraking: "Recibido" },
              { estaActTraking: "Calificado" },
            ],
          },
        },
        {
          $addFields: {
            linea_producto: {
              $arrayElemAt: ["$detalleProducto.linea_producto", 0],
            },
          },
        },
        {
          $match: {
            linea_producto: query.idLinea,
          },
        },
        /** Arma la data de la grafica */
        {
          $addFields: {
            costoTotalProducto: {
              $multiply: ["$unidadesCompradas", "$costoProductos"],
            },
          },
        },
        {
          $facet: {
            miOrganizacion: [
              {
                $match: {
                  idOrganizacion: new ObjectId(query.idOrg),
                },
              },
            ],
            otrasOrganizaciones: [
              {
                $match: {},
              },
            ],
          },
        },
        {
          $project: {
            miOrganizacion: [
              {
                label: "Organización",
                total: { $sum: "$miOrganizacion.costoTotalProducto" },
              },
            ],
            otrasOrganizaciones: [
              {
                label: "Otras organizaciones",
                total: { $sum: "$otrasOrganizaciones.costoTotalProducto" },
              },
            ],
          },
        },
      ]).exec(callback);
    }
  },
  tipoNegocioSector: function (query, tipo_negocio, callback) {
    let ObjectId = require("mongoose").Types.ObjectId;
    if (tipo_negocio.tipo_negocio === "todos") {
      this.aggregate([
        {
          $match: {
            $or: [
              { estaActTraking: "Entregado" },
              { estaActTraking: "Recibido" },
              { estaActTraking: "Calificado" },
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
        /** Recupera el tipo de negocio, la tabla secundaria no lo tiene */
        {
          $lookup: {
            from: "punto_entregas",
            localField: "idPunto", //Nombre del campo de la coleccion actual
            foreignField: "_id", //Nombre del campo de la coleccion a relacionar
            pipeline: [
              {
                $project: {
                  usuario_horeca: "$usuario_horeca",
                },
              },
              {
                $lookup: {
                  from: "usuario_horecas",
                  localField: "usuario_horeca", //Nombre del campo de la coleccion actual
                  foreignField: "_id", //Nombre del campo de la coleccion a relacionar
                  pipeline: [
                    {
                      $project: {
                        tipo_negocio: "$tipo_negocio",
                      },
                    },
                  ],
                  as: "datahoreca", //Nombre del campo donde se insertara todos los documentos relacionados
                },
              },
              {
                $addFields: {
                  datahoreca_: { $arrayElemAt: ["$datahoreca", 0] },
                },
              },
            ],
            as: "dataPunto", //Nombre del campo donde se insertara todos los documentos relacionados
          },
        },
        {
          $addFields: {
            tipo_negocio_: {
              $arrayElemAt: ["$dataPunto.datahoreca_.tipo_negocio", 0],
            },
          },
        },
        {
          $project: {
            tipo_negocio_: "$tipo_negocio_",
            costoTotalProducto: "$costoTotalProducto",
            idOrganizacion: "$idOrganizacion",
          },
        },
        /** Arma la data de la grafica */
        {
          $facet: {
            miOrganizacion: [
              {
                $match: {
                  idOrganizacion: new ObjectId(query.idOrg),
                },
              },
            ],
            otrasOrganizaciones: [
              {
                $match: {},
              },
            ],
          },
        },
        {
          $project: {
            miOrganizacion: [
              {
                label: "Organización",
                total: { $sum: "$miOrganizacion.costoTotalProducto" },
              },
            ],
            otrasOrganizaciones: [
              {
                label: "Otras organizaciones",
                total: { $sum: "$otrasOrganizaciones.costoTotalProducto" },
              },
            ],
          },
        },
      ]).exec(callback);
    } else {
      this.aggregate([
        {
          $match: {
            $or: [
              { estaActTraking: "Entregado" },
              { estaActTraking: "Recibido" },
              { estaActTraking: "Calificado" },
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
        /** Recupera el tipo de negocio, la tabla secundaria no lo tiene */
        {
          $lookup: {
            from: "punto_entregas",
            localField: "idPunto", //Nombre del campo de la coleccion actual
            foreignField: "_id", //Nombre del campo de la coleccion a relacionar
            pipeline: [
              {
                $project: {
                  usuario_horeca: "$usuario_horeca",
                },
              },
              {
                $lookup: {
                  from: "usuario_horecas",
                  localField: "usuario_horeca", //Nombre del campo de la coleccion actual
                  foreignField: "_id", //Nombre del campo de la coleccion a relacionar
                  pipeline: [
                    {
                      $project: {
                        tipo_negocio: "$tipo_negocio",
                      },
                    },
                  ],
                  as: "datahoreca", //Nombre del campo donde se insertara todos los documentos relacionados
                },
              },
              {
                $addFields: {
                  datahoreca_: { $arrayElemAt: ["$datahoreca", 0] },
                },
              },
            ],
            as: "dataPunto", //Nombre del campo donde se insertara todos los documentos relacionados
          },
        },
        {
          $addFields: {
            tipo_negocio_: {
              $arrayElemAt: ["$dataPunto.datahoreca_.tipo_negocio", 0],
            },
          },
        },
        {
          $project: {
            tipo_negocio_: "$tipo_negocio_",
            costoTotalProducto: "$costoTotalProducto",
            idOrganizacion: "$idOrganizacion",
          },
        },
        /** Aplica el filtro de tipo de negocio */
        {
          $match: {
            tipo_negocio_: tipo_negocio.tipo_negocio,
          },
        },
        /** Arma la data de la grafica */
        {
          $facet: {
            miOrganizacion: [
              {
                $match: {
                  idOrganizacion: new ObjectId(query.idOrg),
                },
              },
            ],
            otrasOrganizaciones: [
              {
                $match: {},
              },
            ],
          },
        },
        {
          $project: {
            miOrganizacion: [
              {
                label: "Organización",
                total: { $sum: "$miOrganizacion.costoTotalProducto" },
              },
            ],
            otrasOrganizaciones: [
              {
                label: "Otras organizaciones",
                total: { $sum: "$otrasOrganizaciones.costoTotalProducto" },
              },
            ],
          },
        },
      ]).exec(callback);
    }
  },
  distribuidoresSector: function (query, callback) {
    let ObjectId = require("mongoose").Types.ObjectId;
    if (query.idDist === "todos") {
      this.aggregate([
        {
          $match: {
            $or: [
              { estaActTraking: "Entregado" },
              { estaActTraking: "Recibido" },
              { estaActTraking: "Calificado" },
            ],
          },
        },
        /** Arma la data de la grafica */
        {
          $addFields: {
            costoTotalProducto: {
              $multiply: ["$unidadesCompradas", "$costoProductos"],
            },
          },
        },
        /** Arma la data de la grafica */
        {
          $facet: {
            miOrganizacion: [
              {
                $match: {
                  idOrganizacion: new ObjectId(query.idOrg),
                },
              },
            ],
            otrasOrganizaciones: [
              {
                $match: {},
              },
            ],
          },
        },
        {
          $project: {
            miOrganizacion: [
              {
                label: "Organización",
                total: { $sum: "$miOrganizacion.costoTotalProducto" },
              },
            ],
            otrasOrganizaciones: [
              {
                label: "Otras organizaciones",
                total: { $sum: "$otrasOrganizaciones.costoTotalProducto" },
              },
            ],
          },
        },
      ]).exec(callback);
    } else {
      this.aggregate([
        {
          $match: {
            idDistribuidor: new ObjectId(query.idDist),
          },
        },
        {
          $match: {
            $or: [
              { estaActTraking: "Entregado" },
              { estaActTraking: "Recibido" },
              { estaActTraking: "Calificado" },
            ],
          },
        },
        /** Arma la data de la grafica */
        {
          $addFields: {
            costoTotalProducto: {
              $multiply: ["$unidadesCompradas", "$costoProductos"],
            },
          },
        },
        /** Nuevos campos */
        {
          $facet: {
            miOrganizacion: [
              {
                $match: {
                  idOrganizacion: new ObjectId(query.idOrg),
                },
              },
            ],
            otrasOrganizaciones: [
              {
                $match: {},
              },
            ],
          },
        },
        /** */
        {
          $project: {
            otrasOrganizaciones: [
              {
                label: "Otras organizaciones",
                total: { $sum: "$otrasOrganizaciones.costoTotalProducto" },
              },
            ],
            miOrganizacion: [
              {
                label: "Organización",
                total: { $sum: "$miOrganizacion.costoTotalProducto" },
              },
            ],
          },
        },
      ]).exec(callback);
    }
  },

  removeAll: function (query, callback) {
    this.remove(query).exec(callback);
  },
  create: function (data, callback) {
    const ReportePed = new this(data);
    ReportePed.save(callback);
  },

  establecimientosAlcanzados: function (query, callback) {
    let ObjectId = require("mongoose").Types.ObjectId;
    this.aggregate([
      {
        $match: {
          productoId: new ObjectId(query),
        },
      },
      {
        $project: {
          total: { $size: "$idPunto" },
        },
      },
    ]).exec(callback);
  },

  /** Distribuidores de una organizacion */
  getDistribuidoresOrganizacion: function (query, callback) {
    const d = new Date();
    const fecha_referencia = d.setMonth(d.getMonth() - 3);
    let ObjectId = require("mongoose").Types.ObjectId;
    this.aggregate([
      /** Arma la data de la grafica */
      {
        $match: {
          idOrganizacion: new ObjectId(query),
        },
      },
      {
        $project: {
          idOrganizacion: "$idOrganizacion",
          idDistribuidor: "$idDistribuidor",
          idPedido: "$idPedido",
        },
      },
      {
        $group: {
          _id: "$idDistribuidor",
          organizacion: { $first: "$idOrganizacion" },
          tipoUsuario: { $first: "$tipoUsuario" },
        },
      },
      /** Consutlo los productos de cada distribuidro */
      {
        $lookup: {
          from: "productos_por_distribuidors",
          localField: "_id", //Nombre del campo de la coleccion actual
          foreignField: "distribuidor", //Nombre del campo de la coleccion a relacionar
          pipeline: [
            {
              $project: {
                productos: "$productos",
              },
            },
            {
              $addFields: {
                productos: { $sum: { $size: "$productos" } },
              },
            },
          ],
          as: "data_productos", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
      /** Info general del distribuidor */
      {
        $lookup: {
          from: "distribuidors",
          localField: "_id", //Nombre del campo de la coleccion actual
          foreignField: "_id", //Nombre del campo de la coleccion a relacionar
          as: "data_distribuidor", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
      /** Info. rep. legal */
      {
        $lookup: {
          from: "trabajadors",
          localField: "_id", //Nombre del campo de la coleccion actual
          foreignField: "distribuidor", //Nombre del campo de la coleccion a relacionar
          /* pipeline: [
                  {
                    $match: {
                      tipo_trabajador: "PROPIETARIO/REP LEGAL",
                    },
                  },
                ],*/
          as: "data_trabajadores_global", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
      /** Info. rep. legal */
      {
        $lookup: {
          from: "trabajadors",
          localField: "_id", //Nombre del campo de la coleccion actual
          foreignField: "distribuidor", //Nombre del campo de la coleccion a relacionar
          pipeline: [
            {
              $match: {
                tipo_trabajador: "PROPIETARIO/REP LEGAL",
              },
            },
            {
              $project: {
                nombres: "$nombres",
                apellidos: "$apellidos",
                telefono: "$telefono",
                celular: "$celular",
                tipo_documento: "$tipo_documento",
                numero_documento: "$numero_documento",
              },
            },
          ],
          as: "data_trabajadores", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
      {
        $addFields: {
          data_trabajador: { $arrayElemAt: ["$data_trabajadores", 0] },
        },
      },
      /** Calificación distribuidor */
      {
        $lookup: {
          from: "pedidos",
          localField: "_id", //Nombre del campo de la coleccion actual
          foreignField: "distribuidor", //Nombre del campo de la coleccion a relacionar
          pipeline: [
            {
              $match: {
                estado: "Calificado",
              },
            },
            {
              $project: {
                calificacion: "$calificacion",
                estado: "$estado",
              },
            },
          ],
          as: "data_calificacion", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
      /** Establecimientos Alcanzados */
      {
        $lookup: {
          from: "reportepedidos",
          localField: "_id", //Nombre del campo de la coleccion actual
          foreignField: "idDistribuidor", //Nombre del campo de la coleccion a relacionar
          pipeline: [
            {
              $match: {
                idOrganizacion: new ObjectId(query),
              },
            },
            {
              $match: {
                $or: [
                  { estaActTraking: "Entregado" },
                  { estaActTraking: "Recibido" },
                  { estaActTraking: "Calificado" },
                ],
              },
            },
            {
              $group: {
                _id: {
                  idPunto: "$idPunto",
                  idPedido: "$idPedido",
                },
                total: { $sum: 1 },
              },
            },
          ],
          as: "data_estab_alcanzados", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
      /** Ventas por tipo de punto de entrega */
      {
        $lookup: {
          from: "reportepedidos",
          localField: "_id", //Nombre del campo de la coleccion actual
          foreignField: "idDistribuidor", //Nombre del campo de la coleccion a relacionar
          pipeline: [
            {
              $match: {
                idOrganizacion: new ObjectId(query),
              },
            },
            {
              $match: {
                $or: [
                  { estaActTraking: "Entregado" },
                  { estaActTraking: "Recibido" },
                  { estaActTraking: "Calificado" },
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
              $project: {
                costoTotalProducto: "$costoTotalProducto",
                tipoUsuario: "$tipoUsuario",
              },
            },
            {
              $facet: {
                ventas_bar_disco: [
                  {
                    $match: {
                      tipoUsuario: "BAR / DISCOTECA",
                    },
                  },
                ],
                ventas_cafeteria: [
                  {
                    $match: {
                      tipoUsuario: "CAFETERÍA / HELADERÍA / SNACK",
                    },
                  },
                ],
                ventas_catering: [
                  {
                    $match: {
                      tipoUsuario: "CATERING SERVICE / SERVICIO ALIMENTACIÓN",
                    },
                  },
                ],
                ventas_cocina_oculta: [
                  {
                    $match: {
                      tipoUsuario: "COCINA OCULTA",
                    },
                  },
                ],
                ventas_diversion: [
                  {
                    $match: {
                      tipoUsuario: "CENTRO DE DIVERSIÓN",
                    },
                  },
                ],
                ventas_centro_deportivo_gymnasios: [
                  {
                    $match: {
                      tipoUsuario: "CENTRO DEPORTIVO Y GIMNASIOS",
                    },
                  },
                ],
                ventas_club_social_negocios: [
                  {
                    $match: {
                      tipoUsuario: "CLUB SOCIAL / NEGOCIOS",
                    },
                  },
                ],
                ventas_comedores: [
                  {
                    $match: {
                      tipoUsuario: "COMEDOR DE EMPLEADOS",
                    },
                  },
                ],
                ventas_rapidas: [
                  {
                    $match: {
                      tipoUsuario: "COMIDA RÁPIDA",
                    },
                  },
                ],
                ventas_hogar: [
                  {
                    $match: {
                      tipoUsuario: "HOGAR=",
                    },
                  },
                ],
                ventas_mayoristas: [
                  {
                    $match: {
                      tipoUsuario: "MAYORISTA / MINORISTA",
                    },
                  },
                ],
                ventas_oficina_coworkig: [
                  {
                    $match: {
                      tipoUsuario: "OFICINA / COWORKING",
                    },
                  },
                ],
                ventas_panaderia: [
                  {
                    $match: {
                      tipoUsuario: "PANADERÍA / REPOSTERÍA",
                    },
                  },
                ],

                ventas_propiedad__horizontal: [
                  {
                    $match: {
                      tipoUsuario: "PROPIEDAD HORIZONTAL",
                    },
                  },
                ],
                ventas_cadena: [
                  {
                    $match: {
                      tipoUsuario: "RESTAURANTE DE CADENA",
                    },
                  },
                ],
                ventas_restaurantes: [
                  {
                    $match: {
                      tipoUsuario: "RESTAURANTE",
                    },
                  },
                ],
                ventas_colegios: [
                  {
                    $match: {
                      tipoUsuario: "SECTOR EDUCACIÓN",
                    },
                  },
                ],
                ventas_hoteles: [
                  {
                    $match: {
                      tipoUsuario: "SECTOR HOTELERO",
                    },
                  },
                ],

                ventas_salud: [
                  {
                    $match: {
                      tipoUsuario: "SECTOR SALUD",
                    },
                  },
                ],
                ventas_publico_privado: [
                  {
                    $match: {
                      tipoUsuario: "SECTOR PÚBLICO / PRIVADO",
                    },
                  },
                ],
              },
            },
            {
              $project: {
                ventas_bar_discoteca: {
                  $sum: "$ventas_bar_discoteca.costoTotalProducto",
                },
                ventas_cafeteria: {
                  $sum: "$ventas_cafeteria.costoTotalProducto",
                },
                ventas_catering: {
                  $sum: "$ventas_catering.costoTotalProducto",
                },
                ventas_cocina_oculta: {
                  $sum: "$ventas_cocina_oculta.costoTotalProducto",
                },
                ventas_diversion: {
                  $sum: "$ventas_diversion.costoTotalProducto",
                },
                ventas_centro_deportivo_gymnasios: {
                  $sum: "$ventas_centro_deportivo_gymnasios.costoTotalProducto",
                },
                ventas_club_social_negocios: {
                  $sum: "$ventas_club_social_negocios.costoTotalProducto",
                },
                ventas_comedores: {
                  $sum: "$ventas_comedores.costoTotalProducto",
                },
                ventas_comida_rapida: {
                  $sum: "$ventas_comida_rapida.costoTotalProducto",
                },
                ventas_hogar: {
                  $sum: "$ventas_hogar.costoTotalProducto",
                },
                ventas_mayoristas: {
                  $sum: "$ventas_mayoristas.costoTotalProducto",
                },
                ventas_oficina_coworkig: {
                  $sum: "$ventas_oficina_coworkig.costoTotalProducto",
                },
                ventas_panaderia: {
                  $sum: "$ventas_panaderia.costoTotalProducto",
                },
                ventas_propiedad__horizontal: {
                  $sum: "$ventas_propiedad__horizontal.costoTotalProducto",
                },
                ventas_restaurante: {
                  $sum: "$ventas_restaurante.costoTotalProducto",
                },
                ventas_cadena: {
                  $sum: "$ventas_cadena.costoTotalProducto",
                },
                ventas_colegios: {
                  $sum: "$ventas_colegios.costoTotalProducto",
                },
                ventas_hoteles: {
                  $sum: "$ventas_hoteles.costoTotalProducto",
                },
                ventas_salud: {
                  $sum: "$ventas_salud.costoTotalProducto",
                },
                ventas_publico_privado: {
                  $sum: "$ventas_publico_privado.costoTotalProducto",
                },
              },
            },
          ],
          as: "data_ventas_tipo_punto", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
      /** Ventas por tipo de punto de entrega */
      {
        $lookup: {
          from: "reportepedidos",
          localField: "_id", //Nombre del campo de la coleccion actual
          foreignField: "idDistribuidor", //Nombre del campo de la coleccion a relacionar
          pipeline: [
            {
              $match: {
                idOrganizacion: new ObjectId(query),
              },
            },
            {
              $match: {
                $or: [
                  { estaActTraking: "Entregado" },
                  { estaActTraking: "Recibido" },
                  { estaActTraking: "Calificado" },
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
              $project: {
                costoTotalProducto: { $sum: "$costoTotalProducto" },
              },
            },
          ],
          as: "data_ventas_tres_meses", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
      /** Arma la data a mostrar */
      {
        $project: {
          _idDistribuidor: { $arrayElemAt: ["$data_distribuidor._id", 0] },
          nombre: { $arrayElemAt: ["$data_distribuidor.nombre", 0] },
          razon_social: {
            $arrayElemAt: ["$data_distribuidor.razon_social", 0],
          },
          tipo_persona: {
            $arrayElemAt: ["$data_distribuidor.tipo_persona", 0],
          },
          nit_cc: { $arrayElemAt: ["$data_distribuidor.nit_cc", 0] },
          tipo: { $arrayElemAt: ["$data_distribuidor.tipo", 0] },
          pais: "Colombia",
          departamento: {
            $arrayElemAt: ["$data_distribuidor.departamento", 0],
          },
          ciudad: { $arrayElemAt: ["$data_distribuidor.ciudad", 0] },
          direccion: { $arrayElemAt: ["$data_distribuidor.direccion", 0] },
          correo: { $arrayElemAt: ["$data_distribuidor.correo", 0] },
          celular: { $arrayElemAt: ["$data_distribuidor.celular", 0] },
          telefono: { $arrayElemAt: ["$data_distribuidor.telefono", 0] },
          rep_legal_nombres: "$data_trabajador.nombres",
          rep_legal_apellidos: "$data_trabajador.apellidos",
          rep_legal_telefono: "$data_trabajador.telefono",
          rep_legal_celular: "$data_trabajador.celular",
          rep_legal_tipo_documento: "$data_trabajador.tipo_documento",
          rep_legal_numero_documento: "$data_trabajador.numero_documento",
          descripcion: { $arrayElemAt: ["$data_distribuidor.descripcion", 0] },
          horario_atencion: {
            $arrayElemAt: ["$data_distribuidor.horario_atencion", 0],
          },
          tiempo_entrega: {
            $arrayElemAt: ["$data_distribuidor.tiempo_entrega", 0],
          },
          valor_minimo_pedido: {
            $arrayElemAt: ["$data_distribuidor.valor_minimo_pedido", 0],
          },
          metodo_pago: { $arrayElemAt: ["$data_distribuidor.metodo_pago", 0] },
          data_ventas_tipo_punto: {
            $arrayElemAt: ["$data_ventas_tipo_punto", 0],
          },
          data_ventas_3_meses: {
            $arrayElemAt: ["$data_ventas_tres_meses", 0],
          },
          data_estab_alcanzados: { $size: "$data_estab_alcanzados" },
          total_productos: { $arrayElemAt: ["$data_productos.productos", 0] },
          data_calificacion: "$data_calificacion",
          total_trabajadores: "$data_trabajadores_global",
          trabajadores_rep: "$data_trabajador",
        },
      },
    ]).exec(callback);
  },
  /** Reporte detallado de ventas por producto para una organización */
  reporteDetalladoVentasOrg: function (query, callback) {
    let ObjectId = require("mongoose").Types.ObjectId;
    this.aggregate([
      /** Arma la data de la grafica */
      {
        $match: {
          idOrganizacion: new ObjectId(query),
          $or: [
            { estaActTraking: "Entregado" },
            { estaActTraking: "Recibido" },
            { estaActTraking: "Calificado" },
          ],
        },
      },
      /** Fecha pedido */
      {
        $lookup: {
          from: "pedidos",
          localField: "idPedido",
          foreignField: "_id",
          as: "data_pedido",
        },
      },
      /** Nombre marca producto */
      {
        $lookup: {
          from: "marca_productos", //Nombre de la colecccion a relacionar
          localField: "marcaProducto", //Nombre del campo de la coleccion actual
          foreignField: "_id", //Nombre del campo de la coleccion a relacionar
          pipeline: [],
          as: "marca_producto", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
      /** Nombre categoria producto */
      {
        $lookup: {
          from: "categoria_productos", //Nombre de la colecccion a relacionar
          localField: "categoriaProducto", //Nombre del campo de la coleccion actual
          foreignField: "_id", //Nombre del campo de la coleccion a relacionar
          pipeline: [
            {
              $project: {
                nombre: "$nombre",
              },
            },
          ],
          as: "categoria_producto", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
      /** Linea producto */
      {
        $lookup: {
          from: "productos", //Nombre de la colecccion a relacionar
          localField: "productoId", //Nombre del campo de la coleccion actual
          foreignField: "_id", //Nombre del campo de la coleccion a relacionar
          pipeline: [
            {
              $lookup: {
                from: "linea_productos", //Nombre de la colecccion a relacionar
                localField: "linea_producto", //Nombre del campo de la coleccion actual
                foreignField: "_id", //Nombre del campo de la coleccion a relacionar
                pipeline: [
                  {
                    $project: {
                      nombre: "$nombre",
                    },
                  },
                ],
                as: "linea_producto", //Nombre del campo donde se insertara todos los documentos relacionados
              },
            },
            {
              $unwind: "$linea_producto",
            },
            {
              $replaceRoot: {
                newRoot: "$linea_producto",
              },
            },
          ],
          as: "linea_producto", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
      /** Info general del distribuidor */
      {
        $lookup: {
          from: "distribuidors",
          localField: "idDistribuidor", //Nombre del campo de la coleccion actual
          foreignField: "_id", //Nombre del campo de la coleccion a relacionar
          pipeline: [
            {
              $project: {
                nombre: "$nombre",
                nit_cc: "$nit_cc",
              },
            },
          ],
          as: "data_distribuidor", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
      /**  Info general del punto de entrega */
      {
        $lookup: {
          from: "punto_entregas",
          localField: "idPunto", //Nombre del campo de la coleccion actual
          foreignField: "_id", //Nombre del campo de la coleccion a relacionar
          pipeline: [
            {
              $project: {
                nombre: "$nombre",
                pais: "Colombia",
                departamento: "$departamento",
                ciudad: "$ciudad",
                usuario_horeca: "$usuario_horeca",
              },
            },
          ],
          as: "data_punto", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
      /** Info HORECA */
      {
        $lookup: {
          from: "usuario_horecas",
          localField:
            "data_punto.usuario_horeca" /** Nombre del campo de la coleccion actual */,
          foreignField:
            "_id" /** Nombre del campo de la coleccion a relacionar */,
          pipeline: [
            {
              $project: {
                nombre_establecimiento: "$nombre_establecimiento",
                tipo_usuario: "$tipo_usuario",
                nit: "$nit",
                tipo_negocio: "$tipo_negocio",
              },
            },
          ],
          as: "data_horeca" /** Nombre del campo donde se insertara todos los documentos relacionados */,
        },
      },
      /** Trabajador del distribuidor asignado */
      {
        $lookup: {
          from: "distribuidores_vinculados",
          localField: "idPunto",
          foreignField: "punto_entrega",
          pipeline: [
            {
              $lookup: {
                from: "trabajadors",
                localField: "vendedor", //Nombre del campo de la coleccion actual
                foreignField: "_id", //Nombre del campo de la coleccion a relacionar
                pipeline: [
                  {
                    $project: {
                      nombre: { $concat: ["$nombres", " ", "$apellidos"] },
                    },
                  },
                ],
                as: "data_trabajador", //Nombre del campo donde se insertara todos los documentos relacionados
              },
            },
            {
              $project: {
                distribuidor: "$distribuidor",
                trabajador_asignado: {
                  $arrayElemAt: ["$data_trabajador.nombre", 0],
                },
              },
            },
          ],
          as: "data_vinculacion",
        },
      },
      {
        $addFields: {
          producto_precios: {
            $arrayElemAt: ["$detalleProducto.precios", 0],
          },
        },
      },
      /** Arma la data a mostrar */
      {
        $project: {
          distribuidor_nombre: {
            $arrayElemAt: ["$data_distribuidor.nombre", 0],
          },
          distribuidor_id: "$idDistribuidor",
          distribuidor_nit_cc: {
            $arrayElemAt: ["$data_distribuidor.nit_cc", 0],
          },
          horeca_nombre_establecimiento: {
            $arrayElemAt: ["$data_horeca.nombre_establecimiento", 0],
          },
          horeca_tipo_usuario: {
            $arrayElemAt: ["$data_horeca.tipo_usuario", 0],
          },
          horeca_nit: {
            $arrayElemAt: ["$data_horeca.nit", 0],
          },
          horeca_tipo_negocio: {
            $arrayElemAt: ["$data_horeca.tipo_negocio", 0],
          },
          punto_nombre: {
            $arrayElemAt: ["$data_punto.nombre", 0],
          },
          punto_pais: "Colombia",
          punto_departamento: {
            $arrayElemAt: ["$data_punto.departamento", 0],
          },
          punto_ciudad: {
            $arrayElemAt: ["$data_punto.ciudad", 0],
          },
          pedido_id: {
            $arrayElemAt: ["$data_pedido.id_pedido", 0],
          },
          pedido_fecha: "$createdAt",
          categoria_producto: {
            $arrayElemAt: ["$categoria_producto.nombre", 0],
          },
          linea_producto: {
            $arrayElemAt: ["$linea_producto.nombre", 0],
          },
          marca_producto: {
            $arrayElemAt: ["$marca_producto.nombre", 0],
          },
          producto_codigo_distribuidor: "$codigoDistribuidorProducto",
          codigo_organizacion: "$codigo_organizacion_producto",
          producto_descripcion: "$detalleProducto.descripcion",
          nombre_producto: "$detalleProducto.nombre",
          producto_precios: "$producto_precios",
          producto_und_x_caja: "$producto_precios.und_x_caja",
          producto_cantidad_medida: "$producto_precios.cantidad_medida",
          producto_unidad_medida: "$producto_precios.unidad_medida",
          producto_precio_unidad: "$producto_precios.precio_unidad",
          venta_cajas: "$caja",
          venta_unidades: "$unidadesCompradas",
          venta_costo: {
            $multiply: [
              "$unidadesCompradas",
              "$producto_precios.precio_unidad",
            ],
          },
          venta_puntos_FT: {
            $multiply: [
              "$unidadesCompradas",
              "$producto_precios.puntos_ft_unidad",
            ],
          },
          data_vinculacion: "$data_vinculacion",
          estado_pedido: "$data_pedido.estado",
        },
      },
    ]).exec(callback);
  },
  reporte_detallado_ventas_pf_org: function (query, callback) {
    let ObjectId = require("mongoose").Types.ObjectId;
    this.aggregate([
      /** Arma la data de la grafica */
      {
        $match: {
          idOrganizacion: new ObjectId(query),
          estaActTraking: { $nin: ['Rechazado', 'Cancelado por horeca', 'Cancelado por distribuidor', 'Sugerido'] }
        },
      },
      /** Fecha pedido */
      {
        $lookup: {
          from: "pedidos",
          localField: "idPedido",
          foreignField: "_id",
          as: "data_pedido",
        },
      },
      /** Nombre marca producto */
      {
        $lookup: {
          from: "marca_productos", //Nombre de la colecccion a relacionar
          localField: "marcaProducto", //Nombre del campo de la coleccion actual
          foreignField: "_id", //Nombre del campo de la coleccion a relacionar
          pipeline: [],
          as: "marca_producto", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
      /** Nombre categoria producto */
      {
        $lookup: {
          from: "categoria_productos", //Nombre de la colecccion a relacionar
          localField: "categoriaProducto", //Nombre del campo de la coleccion actual
          foreignField: "_id", //Nombre del campo de la coleccion a relacionar
          pipeline: [
            {
              $project: {
                nombre: "$nombre",
              },
            },
          ],
          as: "categoria_producto", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
      /** Linea producto */
      {
        $lookup: {
          from: "productos", //Nombre de la colecccion a relacionar
          localField: "productoId", //Nombre del campo de la coleccion actual
          foreignField: "_id", //Nombre del campo de la coleccion a relacionar
          pipeline: [
            {
              $lookup: {
                from: "linea_productos", //Nombre de la colecccion a relacionar
                localField: "linea_producto", //Nombre del campo de la coleccion actual
                foreignField: "_id", //Nombre del campo de la coleccion a relacionar
                pipeline: [
                  {
                    $project: {
                      nombre: "$nombre",
                    },
                  },
                ],
                as: "linea_producto", //Nombre del campo donde se insertara todos los documentos relacionados
              },
            },
            {
              $unwind: "$linea_producto",
            },
            {
              $replaceRoot: {
                newRoot: "$linea_producto",
              },
            },
          ],
          as: "linea_producto", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
      /** Info general del distribuidor */
      {
        $lookup: {
          from: "distribuidors",
          localField: "idDistribuidor", //Nombre del campo de la coleccion actual
          foreignField: "_id", //Nombre del campo de la coleccion a relacionar
          pipeline: [
            {
              $project: {
                nombre: "$nombre",
                nit_cc: "$nit_cc",
              },
            },
          ],
          as: "data_distribuidor", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
      /**  Info general del punto de entrega */
      {
        $lookup: {
          from: "punto_entregas",
          localField: "idPunto", //Nombre del campo de la coleccion actual
          foreignField: "_id", //Nombre del campo de la coleccion a relacionar
          pipeline: [
            {
              $project: {
                nombre: "$nombre",
                pais: "Colombia",
                departamento: "$departamento",
                ciudad: "$ciudad",
                usuario_horeca: "$usuario_horeca",
              },
            },
          ],
          as: "data_punto", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
      /** Info HORECA */
      {
        $lookup: {
          from: "usuario_horecas",
          localField:
            "data_punto.usuario_horeca" /** Nombre del campo de la coleccion actual */,
          foreignField:
            "_id" /** Nombre del campo de la coleccion a relacionar */,
          pipeline: [
            {
              $project: {
                nombre_establecimiento: "$nombre_establecimiento",
                tipo_usuario: "$tipo_usuario",
                nit: "$nit",
                tipo_negocio: "$tipo_negocio",
              },
            },
          ],
          as: "data_horeca" /** Nombre del campo donde se insertara todos los documentos relacionados */,
        },
      },
      /** Trabajador del distribuidor asignado */
      {
        $lookup: {
          from: "distribuidores_vinculados",
          localField: "idPunto",
          foreignField: "punto_entrega",
          pipeline: [
            {
              $lookup: {
                from: "trabajadors",
                localField: "vendedor", //Nombre del campo de la coleccion actual
                foreignField: "_id", //Nombre del campo de la coleccion a relacionar
                pipeline: [
                  {
                    $project: {
                      nombre: { $concat: ["$nombres", " ", "$apellidos"] },
                    },
                  },
                ],
                as: "data_trabajador", //Nombre del campo donde se insertara todos los documentos relacionados
              },
            },
            {
              $project: {
                distribuidor: "$distribuidor",
                trabajador_asignado: {
                  $arrayElemAt: ["$data_trabajador.nombre", 0],
                },
              },
            },
          ],
          as: "data_vinculacion",
        },
      },
      {
        $addFields: {
          producto_precios: {
            $arrayElemAt: ["$detalleProducto.precios", 0],
          },
        },
      },
      /** Arma la data a mostrar */
      {
        $project: {
          distribuidor_nombre: {
            $arrayElemAt: ["$data_distribuidor.nombre", 0],
          },
          distribuidor_id: "$idDistribuidor",
          distribuidor_nit_cc: {
            $arrayElemAt: ["$data_distribuidor.nit_cc", 0],
          },
          horeca_nombre_establecimiento: {
            $arrayElemAt: ["$data_horeca.nombre_establecimiento", 0],
          },
          horeca_tipo_usuario: {
            $arrayElemAt: ["$data_horeca.tipo_usuario", 0],
          },
          horeca_nit: {
            $arrayElemAt: ["$data_horeca.nit", 0],
          },
          horeca_tipo_negocio: {
            $arrayElemAt: ["$data_horeca.tipo_negocio", 0],
          },
          punto_nombre: {
            $arrayElemAt: ["$data_punto.nombre", 0],
          },
          punto_pais: "Colombia",
          punto_departamento: {
            $arrayElemAt: ["$data_punto.departamento", 0],
          },
          punto_ciudad: {
            $arrayElemAt: ["$data_punto.ciudad", 0],
          },
          pedido_id: {
            $arrayElemAt: ["$data_pedido.id_pedido", 0],
          },
          pedido_fecha: "$createdAt",
          categoria_producto: {
            $arrayElemAt: ["$categoria_producto.nombre", 0],
          },
          linea_producto: {
            $arrayElemAt: ["$linea_producto.nombre", 0],
          },
          marca_producto: {
            $arrayElemAt: ["$marca_producto.nombre", 0],
          },
          producto_codigo_distribuidor: "$codigoDistribuidorProducto",
          codigo_organizacion: "$codigo_organizacion_producto",
          producto_descripcion: "$detalleProducto.descripcion",
          nombre_producto: "$detalleProducto.nombre",
          producto_precios: "$producto_precios",
          producto_und_x_caja: "$producto_precios.und_x_caja",
          producto_cantidad_medida: "$producto_precios.cantidad_medida",
          producto_unidad_medida: "$producto_precios.unidad_medida",
          producto_precio_unidad: "$producto_precios.precio_unidad",
          venta_cajas: "$caja",
          venta_unidades: "$unidadesCompradas",
          venta_costo: {
            $multiply: [
              "$unidadesCompradas",
              "$producto_precios.precio_unidad",
            ],
          },
          venta_puntos_FT: {
            $multiply: [
              "$unidadesCompradas",
              "$producto_precios.puntos_ft_unidad",
            ],
          },
          data_vinculacion: "$data_vinculacion",
          estado_pedido: "$data_pedido.estado",
          puntosGanados: {
            $multiply: [
              "$unidadesCompradas",
              "$puntos_ft_unidad",
            ],
          },
          fechaInicio: "$detalleProducto.fecha_apertura_puntosft",
          fechaFin: "$detalleProducto.fecha_cierre_puntosft",

        },
      },
    ]).exec(callback);
  },
  /** Reporte detallado de puntos por producto - organización */
  reportePuntosFeatOrg: function (query, callback) {
    let ObjectId = require("mongoose").Types.ObjectId;
    this.aggregate([
      {
        $match: {
          idOrganizacion: new ObjectId(query),
        },
      },
      /** Data de los pedidos */
      {
        $lookup: {
          from: "pedidos",
          localField: "idPedido",
          foreignField: "_id",
          as: "data_pedido",
        },
      },
      {
        $addFields: {
          fecha_pedido: { $arrayElemAt: ["$data_pedido", 0] },
        },
      },
      /** agrega la fecha del pedido */
      {
        $addFields: {
          producto_fecha_pedido: {
            $dateToString: {
              format: "%Y-%m",
              date: "$fecha_pedido.createdAt",
            },
          },
        },
      },
      /** Productos */
      {
        $lookup: {
          from: "productos",
          localField: "productoId",
          foreignField: "_id",
          as: "producto_data_actual",
        },
      },
      {
        $addFields: {
          linea_producto: {
            $arrayElemAt: ["$producto_data_actual.linea_producto", 0],
          },
        },
      },
      /** Categorias */
      {
        $lookup: {
          from: "categoria_productos",
          localField: "categoriaProducto",
          foreignField: "_id",
          as: "data_categoria",
        },
      },
      /** Lineas */
      {
        $lookup: {
          from: "linea_productos",
          localField: "linea_producto",
          foreignField: "_id",
          as: "data_linea",
        },
      },
      /** Marcas */
      {
        $lookup: {
          from: "marca_productos",
          localField: "marcaProducto",
          foreignField: "_id",
          as: "data_marca",
        },
      },
      // Putos acumulados, retora cuantas unidades aplica para puntos ft, relacionado a si el pedido fue entregado
      {
        $lookup: {
          let: {
            unidadesCompradas: "$unidadesCompradas",
            puntos_ft_unidad: "$puntos_ft_unidad",
          },
          from: "pedidos",
          localField: "idPedido",
          foreignField: "_id",
          pipeline: [
            {
              $match: {
                $or: [
                  { estado: "Entregado" },
                  { estado: "Recibido" },
                  { estado: "Calificado" },
                ],
              },
            },
            {
              $project: {
                unidadesCompradas: "$$unidadesCompradas",
              },
            },
          ],
          as: "data_unidades_con_puntos",
        },
      },
      {
        $project: {
          unidades_que_acumulan: {
            $sum: "$data_unidades_con_puntos.unidadesCompradas",
          },
          producto_id: "$productoId",
          producto_fecha_pedido: "$producto_fecha_pedido",
          producto_cantidad_vendida: "$unidadesCompradas",
          producto_puntos_venta_und: "$puntos_ft_unidad",
          producto_codigo_organizacion: "$codigo_organizacion_producto",
          producto_nombre: {
            $arrayElemAt: ["$producto_data_actual.nombre", 0],
          },
          producto_precios_mes: {
            $arrayElemAt: ["$detalleProducto.precios", 0],
          },
          producto_precios_actual: {
            $arrayElemAt: ["$producto_data_actual.precios", 0],
          },
          producto_inicio_puntos: {
            $arrayElemAt: ["$producto_data_actual.fecha_apertura_puntosft", 0],
          },
          producto_cierre_puntos: {
            $arrayElemAt: ["$producto_data_actual.fecha_cierre_puntosft", 0],
          },
          producto_descripcion: {
            $arrayElemAt: ["$producto_data_actual.descripcion", 0],
          },
          categoria: {
            $arrayElemAt: ["$data_categoria.nombre", 0],
          },
          linea: {
            $arrayElemAt: ["$data_linea.nombre", 0],
          },
          marca: {
            $arrayElemAt: ["$data_marca.nombre", 0],
          },
        },
      },
      {
        $group: {
          _id: {
            producto_fecha_pedido: "$producto_fecha_pedido",
            producto_codigo_organizacion: "$producto_codigo_organizacion",
          },
          unidades_que_acumulan: { $sum: "$unidades_que_acumulan" },
          producto_cantidad_vendida: { $sum: "$producto_cantidad_vendida" },
          producto_puntos_venta_und: { $first: "$producto_puntos_venta_und" },
          producto_id: {
            $first: "$producto_id",
          },
          producto_codigo_organizacion: {
            $first: "$producto_codigo_organizacion",
          },
          producto_nombre: { $first: "$producto_nombre" },
          /** Son los puntos que tenia cuando se hizo el pedido */
          producto_puntos_mes: {
            $first: "$producto_precios_mes.puntos_ft_unidad",
          },
          /** Son los puntos que tienen cuando se hace la consulta */
          producto_inicio_puntos_actual: { $first: "$producto_inicio_puntos" },
          producto_cierre_puntos_actual: { $first: "$producto_cierre_puntos" },
          producto_puntos_actual: {
            $first: {
              $arrayElemAt: ["$producto_precios_actual.puntos_ft_unidad", 0],
            },
          },
          producto_descripcion: { $first: "$producto_descripcion" },
          categoria: { $first: "$categoria" },
          linea: { $first: "$linea" },
          marca: { $first: "$marca" },
        },
      },
    ]).exec(callback);
  },
  /** Reporte detallado de puntos de ventas y sus establecimiento que han comprado a una organizacion */
  reportePuntosEstablecimientos: function (query, callback) {
    const d = new Date();
    const fecha_referencia = d.setMonth(d.getMonth() - 3);
    let ObjectId = require("mongoose").Types.ObjectId;
    this.aggregate([
      /** Arma la data de la grafica */
      {
        $match: {
          idOrganizacion: new ObjectId(query),
        },
      },
      {
        $match: {
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
      /** Info general del punto de entrega */
      {
        $lookup: {
          from: "punto_entregas",
          localField: "_id", //Nombre del campo de la coleccion actual
          foreignField: "_id", //Nombre del campo de la coleccion a relacionar
          pipeline: [
            {
              $project: {
                punto_pais: "Colombia",
                punto_departamento: "$departamento",
                punto_ciudad: "$ciudad",
                punto_nombre: "$nombre",
                punto_sillas: "$sillas",
                punto_domicilios: "$domicilios",
                usuario_horeca: "$usuario_horeca",
              },
            },
          ],
          as: "data_punto", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
      /** Info HORECA */
      {
        $lookup: {
          from: "usuario_horecas",
          localField:
            "data_punto.usuario_horeca" /** Nombre del campo de la coleccion actual */,
          foreignField:
            "_id" /** Nombre del campo de la coleccion a relacionar */,
          pipeline: [
            {
              $project: {
                horeca_tipo_negocio: "$tipo_negocio",
                horeca_tipo_usuario: "$tipo_usuario",
                horeca_nit: "$nit",
                horeca_razon_social: "$razon_social",
                horeca_nombre: "$nombre_establecimiento",
              },
            },
          ],
          as: "data_horeca" /** Nombre del campo donde se insertara todos los documentos relacionados */,
        },
      },
      //Ventas en los ultimos 3 meses
      {
        $lookup: {
          from: "reportepedidos",
          localField: "_id" /** Nombre del campo de la coleccion actual */,
          foreignField:
            "idPunto" /** Nombre del campo de la coleccion a relacionar */,
          pipeline: [
            {
              $match: {
                idOrganizacion: new ObjectId(query),
                createdAt: {
                  $gte: new Date(fecha_referencia),
                },

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
                _id: {
                  idPunto: "$idPunto",
                  productoId: "$productoId",
                },
                total: { $sum: "$costoTotalProducto" },
              },
            },
          ],
          as: "data_ventas" /** Nombre del campo donde se insertara todos los documentos relacionados */,
        },
      },
      //Referencias en los ultimos 3 meses
      {
        $lookup: {
          from: "reportepedidos",
          localField: "_id" /** Nombre del campo de la coleccion actual */,
          foreignField:
            "idPunto" /** Nombre del campo de la coleccion a relacionar */,
          pipeline: [

            {
              $match: {
                idOrganizacion: new ObjectId(query),
                createdAt: {
                  $gte: new Date(fecha_referencia),
                },
              },
            },
            {
              $group: {
                _id: {
                  idPunto: "$idPunto",
                  productoId: "$productoId",
                },
                total: { $sum: 1 },
              },
            },
          ],
          as: "data_referencias" /** Nombre del campo donde se insertara todos los documentos relacionados */,
        },
      },
      //Arma la data a mostrar
      {
        $project: {
          punto_pais: {
            $arrayElemAt: ["$data_punto.punto_pais", 0],
          },
          punto_departamento: {
            $arrayElemAt: ["$data_punto.punto_departamento", 0],
          },
          punto_ciudad: {
            $arrayElemAt: ["$data_punto.punto_ciudad", 0],
          },
          punto_nombre: {
            $arrayElemAt: ["$data_punto.punto_nombre", 0],
          },
          punto_sillas: {
            $arrayElemAt: ["$data_punto.punto_sillas", 0],
          },
          punto_domicilios: {
            $arrayElemAt: ["$data_punto.punto_domicilios", 0],
          },
          distribuidor_id: "$idDistribuidor",
          horeca_tipo_negocio: {
            $arrayElemAt: ["$data_horeca.horeca_tipo_negocio", 0],
          },
          horeca_tipo_usuario: {
            $arrayElemAt: ["$data_horeca.horeca_tipo_usuario", 0],
          },
          horeca_nit: {
            $arrayElemAt: ["$data_horeca.horeca_nit", 0],
          },
          horeca_razon_social: {
            $arrayElemAt: ["$data_horeca.horeca_razon_social", 0],
          },
          horeca_nombre: {
            $arrayElemAt: ["$data_horeca.horeca_nombre", 0],
          },
          data_ventas_tres_meses: { $sum: "$data_ventas.total" },
          data_referencias_tres_meses: { $size: "$data_referencias" },
          idPunto: "$_id",

        },
      },
    ]).exec(callback);
  },
  /***************** Sector ****************/
  sectorPuntosEntregaXCiudad: function (query, callback) {
    let ObjectId = require("mongoose").Types.ObjectId;
    this.aggregate([
      {
        $match: {
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
      /** Info general del punto de entrega */
      {
        $lookup: {
          from: "punto_entregas",
          localField: "_id", //Nombre del campo de la coleccion actual
          foreignField: "_id", //Nombre del campo de la coleccion a relacionar
          pipeline: [
            {
              $project: {
                punto_ciudad: "$ciudad",
              },
            },
          ],
          as: "data_punto", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
      /** Agrupa la ciudad */
      {
        $project: {
          punto_ciudad: {
            $arrayElemAt: ["$data_punto.punto_ciudad", 0],
          },
        },
      },
      {
        $group: {
          _id: "$punto_ciudad",
          total: { $sum: 1 },
        },
      },
      {
        $sort: { total: -1 },
      },
    ]).exec(callback);
  },
  /** Reporte de tipos de negocio que han vendido productos */
  sectorTiposNegocioXOrg: function (query, callback) {
    let ObjectId = require("mongoose").Types.ObjectId;
    this.aggregate([
      {
        $group: {
          _id: "$idPunto",
        },
      },
      /** Info general del punto de entrega */
      {
        $lookup: {
          from: "punto_entregas",
          localField: "_id", //Nombre del campo de la coleccion actual
          foreignField: "_id", //Nombre del campo de la coleccion a relacionar
          pipeline: [
            {
              $project: {
                punto_horeca: "$usuario_horeca",
              },
            },
          ],
          as: "data_punto_entrega", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
      /** Info HORECA */
      {
        $project: {
          horeca_usuario: {
            $arrayElemAt: ["$data_punto_entrega.punto_horeca", 0],
          },
        },
      },
      {
        $lookup: {
          from: "usuario_horecas",
          localField:
            "horeca_usuario" /** Nombre del campo de la coleccion actual */,
          foreignField:
            "_id" /** Nombre del campo de la coleccion a relacionar */,
          pipeline: [
            {
              $project: {
                horeca_tipo_negocio: "$tipo_negocio",
                horeca_nombre: "$nombre_establecimiento",
              },
            },
          ],
          as: "data_horeca" /** Nombre del campo donde se insertara todos los documentos relacionados */,
        },
      },
      {
        $project: {
          horeca_usuario: "$horeca_usuario",
          horeca_tipo_negocio: {
            $arrayElemAt: ["$data_horeca.horeca_tipo_negocio", 0],
          },
          horeca_nombre: {
            $arrayElemAt: ["$data_horeca.horeca_nombre", 0],
          },
        },
      },
      {
        $facet: {
          ventas_diversion: [
            {
              $match: {
                horeca_tipo_negocio: "CENTRO DE DIVERSIÓN",
              },
            },
          ],
          ventas_cafeteria: [
            {
              $match: {
                horeca_tipo_negocio: "CAFETERÍA / HELADERÍA / SNACK",
              },
            },
          ],
          ventas_catering: [
            {
              $match: {
                horeca_tipo_negocio: "CATERING SERVICE / SERVICIO ALIMENTACIÓN",
              },
            },
          ],
          ventas_colegios: [
            {
              $match: {
                horeca_tipo_negocio: "COLEGIO / UNIVERSIDAD / CLUB",
              },
            },
          ],
          ventas_comedores: [
            {
              $match: {
                horeca_tipo_negocio: "COMEDOR DE EMPLEADOS",
              },
            },
          ],
          ventas_rapidas: [
            {
              $match: {
                horeca_tipo_negocio: "COMIDA RÁPIDA",
              },
            },
          ],
          ventas_hoteles: [
            {
              $match: {
                horeca_tipo_negocio: "HOTEL / HOSPITAL",
              },
            },
          ],
          ventas_mayoristas: [
            {
              $match: {
                horeca_tipo_negocio: "MAYORISTA / MINORISTA",
              },
            },
          ],
          ventas_panaderia: [
            {
              $match: {
                horeca_tipo_negocio: "PANADERÍA / REPOSTERÍA",
              },
            },
          ],
          ventas_restaurantes: [
            {
              $match: {
                horeca_tipo_negocio: "RESTAURANTE",
              },
            },
          ],
          ventas_cadena: [
            {
              $match: {
                horeca_tipo_negocio: "RESTAURANTE DE CADENA",
              },
            },
          ],
        },
      },
      {
        $project: {
          ventas_diversion: { $size: "$ventas_diversion" },
          ventas_cafeteria: { $size: "$ventas_cafeteria" },
          ventas_catering: { $size: "$ventas_catering" },
          ventas_colegios: { $size: "$ventas_colegios" },
          ventas_comedores: { $size: "$ventas_comedores" },
          ventas_rapidas: { $size: "$ventas_rapidas" },
          ventas_hoteles: { $size: "$ventas_hoteles" },
          ventas_mayoristas: { $size: "$ventas_mayoristas" },
          ventas_panaderia: { $size: "$ventas_panaderia" },
          ventas_restaurantes: { $size: "$ventas_restaurantes" },
          ventas_cadena: { $size: "$ventas_cadena" },
        },
      },
    ]).exec(callback);
  },
  /** Reporte de categorias de productos que se han vendido de la organizacion */
  sectorCategoriasProducto: function (query, callback) {
    let ObjectId = require("mongoose").Types.ObjectId;
    this.aggregate([
      {
        $group: {
          _id: {
            pedido: "$idPedido",
            usuario: "$idUserHoreca",
            categoria: "$categoriaProducto",
          },
        },
      },
      {
        $lookup: {
          from: "categoria_productos",
          localField: "_id.categoria",
          foreignField: "_id",
          pipeline: [
            {
              $project: {
                nombre_categoria: "$nombre",
              },
            },
          ],
          as: "data_categoria",
        },
      },
      {
        $group: {
          _id: "$data_categoria.nombre_categoria",
          count: { $sum: 1 },
        },
      },
      {
        $sort: { count: -1 },
      },
      /*
      // Info general del punto de entrega
      {
        $lookup: {
          from: "productos",
          localField: "_id", //Nombre del campo de la coleccion actual
          foreignField: "_id", //Nombre del campo de la coleccion a relacionar
          pipeline: [
            {
              $project: {
                categoria_producto: "$categoria_producto",
              },
            },
          ],
          as: "data_producto", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
      // Info Prodcuto
      {
        $project: {
          categoria_producto: {
            $arrayElemAt: ["$data_producto.categoria_producto", 0],
          },
        },
      },
      {
        $lookup: {
          from: "categoria_productos",
          localField:
            "categoria_producto",
          foreignField:
            "_id",
          pipeline: [
            {
              $project: {
                nombre_categoria: "$nombre",
              },
            },
          ],
          as: "data_categoria",
        },
      },
      {
        $project: {
          nombre_categoria: {
            $arrayElemAt: ["$data_categoria.nombre_categoria", 0],
          },
        },
      },
      //Arregla data a mostrar
      {
        $group: {
          _id: "$nombre_categoria",
          count: { $sum: 1 },
        },
      },
      {
        $sort: { count: -1 },
      },*/
    ]).exec(callback);
  },
  /** Reporte de sillas de puntos por ciudad que han vendido productos de la organizacion */
  sectorSillasCiudad: function (query, callback) {
    let ObjectId = require("mongoose").Types.ObjectId;
    this.aggregate([
      {
        $group: {
          _id: "$idPunto",
        },
      },
      /** Info del punto de entrega */
      {
        $lookup: {
          from: "punto_entregas",
          localField: "_id", //Nombre del campo de la coleccion actual
          foreignField: "_id", //Nombre del campo de la coleccion a relacionar
          pipeline: [
            {
              $project: {
                punto_ciudad: "$ciudad",
                punto_sillas: "$sillas",
              },
            },
          ],
          as: "data_punto", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
      {
        $project: {
          punto_ciudad: {
            $arrayElemAt: ["$data_punto.punto_ciudad", 0],
          },
          punto_sillas: {
            $arrayElemAt: ["$data_punto.punto_sillas", 0],
          },
        },
      },
      /** Arregla data a mostrar */
      {
        $group: {
          _id: "$punto_ciudad",
          total: { $sum: "$punto_sillas" },
        },
      },
      {
        $sort: { total: -1 },
      },
    ]).exec(callback);
  },
  /** Reporte de sillas de tipo de negocio que han vendido productos de la organizacion */
  sectorSillasTipoNegocio: function (query, callback) {
    let ObjectId = require("mongoose").Types.ObjectId;
    this.aggregate([
      {
        $group: {
          _id: "$idPunto",
        },
      },
      /** Punto de entrega */
      {
        $lookup: {
          from: "punto_entregas",
          localField: "_id", //Nombre del campo de la coleccion actual
          foreignField: "_id", //Nombre del campo de la coleccion a relacionar
          pipeline: [
            {
              $project: {
                punto_usuario_horeca: "$usuario_horeca",
                punto_sillas: "$sillas",
              },
            },
          ],
          as: "data_punto", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
      /** Horeca */
      {
        $lookup: {
          from: "usuario_horecas",
          localField:
            "data_punto.punto_usuario_horeca" /** Nombre del campo de la coleccion actual */,
          foreignField:
            "_id" /** Nombre del campo de la coleccion a relacionar */,
          pipeline: [
            {
              $project: {
                horeca_tipo_negocio: "$tipo_negocio",
                horeca_nombre: "$nombre_establecimiento",
              },
            },
          ],
          as: "data_horeca" /** Nombre del campo donde se insertara todos los documentos relacionados */,
        },
      },
      /** Arreglo de datos a mostrar */
      {
        $project: {
          punto_usuario_horeca: {
            $arrayElemAt: ["$data_punto.punto_usuario_horeca", 0],
          },
          punto_sillas: {
            $arrayElemAt: ["$data_punto.punto_sillas", 0],
          },
          horeca_tipo_negocio: {
            $arrayElemAt: ["$data_horeca.horeca_tipo_negocio", 0],
          },
          horeca_nombre: {
            $arrayElemAt: ["$data_horeca.horeca_nombre", 0],
          },
        },
      },
      /** Se agrupan los datos */
      {
        $group: {
          _id: "$horeca_tipo_negocio",
          total: { $sum: "$punto_sillas" },
        },
      },
      {
        $sort: { total: -1 },
      },
    ]).exec(callback);
  },
  /** Reporte de sillas por categoria de productos vendidos de la organizacion */
  sectorSillasCategoria: function (query, callback) {
    let ObjectId = require("mongoose").Types.ObjectId;
    this.aggregate([
      /*{
        $group: {
          _id: "$productoId",
          punto_id: { $first: "$idPunto" },
          categoria_id: { $first: "$categoriaProducto" },
        },
      },
      {
        $lookup: {
          from: "categoria_productos",
          localField: "categoria_id", //Nombre del campo de la coleccion actual
          foreignField: "_id", //Nombre del campo de la coleccion a relacionar
          pipeline: [
            {
              $project: {
                categoria_nombre: "$nombre",
              },
            },
          ],
          as: "data_categoria", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
      {
        $lookup: {
          from: "punto_entregas",
          localField: "punto_id", //Nombre del campo de la coleccion actual
          foreignField: "_id", //Nombre del campo de la coleccion a relacionar
          pipeline: [
            {
              $project: {
                punto_sillas: "$sillas",
              },
            },
          ],
          as: "data_punto", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
      {
        $project: {
          categoria_nombre: {
            $arrayElemAt: ["$data_categoria.categoria_nombre", 0],
          },
          punto_sillas: {
            $arrayElemAt: ["$data_punto.punto_sillas", 0],
          },
          punto_id: "$punto_id",
          categoria_id: "$categoria_id",
        },
      },
      {
        $group: {
          _id: "$categoria_nombre",
          total: { $sum: "$punto_sillas" },
        },
      },
      {
        $sort: { total: -1 },
      },*/
      {
        $group: {
          _id: { pedido: "$idPedido", usuario: "$idUserHoreca" },
          punto_id: { $first: "$idPunto" },
          categoria_id: { $first: "$categoriaProducto" },
        },
      },
      {
        $lookup: {
          from: "categoria_productos",
          localField: "categoria_id", //Nombre del campo de la coleccion actual
          foreignField: "_id", //Nombre del campo de la coleccion a relacionar
          pipeline: [
            {
              $project: {
                categoria_nombre: "$nombre",
              },
            },
          ],
          as: "data_categoria", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
      {
        $lookup: {
          from: "punto_entregas",
          localField: "punto_id", //Nombre del campo de la coleccion actual
          foreignField: "_id", //Nombre del campo de la coleccion a relacionar
          pipeline: [
            {
              $project: {
                punto_sillas: "$sillas",
              },
            },
          ],
          as: "data_punto", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
      {
        $project: {
          categoria_nombre: {
            $arrayElemAt: ["$data_categoria.categoria_nombre", 0],
          },
          punto_sillas: {
            $arrayElemAt: ["$data_punto.punto_sillas", 0],
          },
          punto_id: "$punto_id",
          categoria_id: "$categoria_id",
        },
      },
      {
        $group: {
          _id: "$categoria_nombre",
          total: { $sum: "$punto_sillas" },
        },
      },
      {
        $sort: { total: -1 },
      },
    ]).exec(callback);
  },
  /******************** Home ********************/
  /** Reporte de puntos de entrega que han vendido productos de una orga por ciudad */
  puntosEntregaXCiudad: function (query, callback) {
    let ObjectId = require("mongoose").Types.ObjectId;
    this.aggregate([
      /** Arma la data de la grafica */
      {
        $match: {
          idOrganizacion: new ObjectId(query),
        },
      },
      {
        $group: {
          _id: "$idPunto",
        },
      },
      /** Info general del punto de entrega */
      {
        $lookup: {
          from: "punto_entregas",
          localField: "_id", //Nombre del campo de la coleccion actual
          foreignField: "_id", //Nombre del campo de la coleccion a relacionar
          pipeline: [
            {
              $project: {
                punto_ciudad: "$ciudad",
              },
            },
          ],
          as: "data_punto", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
      /** Agrupa la ciudad */
      {
        $project: {
          punto_ciudad: {
            $arrayElemAt: ["$data_punto.punto_ciudad", 0],
          },
        },
      },
      {
        $group: {
          _id: "$punto_ciudad",
          total: { $sum: 1 },
        },
      },
    ]).exec(callback);
  },
  /** Reporte de tipos de negocio que han vendido productos */
  tiposNegocioXOrg: function (query, callback) {
    let ObjectId = require("mongoose").Types.ObjectId;
    let dateActual = Date();
    //let dateActualInicio = dateActual-6('M');
    this.aggregate([
      {
        $match: {
          idOrganizacion: new ObjectId(query),
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
          total: { $sum: 1 },
          idUserHoreca: { $last: "$idUserHoreca" },
        },
      },
      {
        $lookup: {
          from: "usuario_horecas",
          localField:
            "idUserHoreca",
          foreignField:
            "_id",
          pipeline: [
            {
              $project: {
                _id: "$_id",
                horeca_tipo_negocio: "$tipo_negocio",
                horeca_nombre: "$nombre_establecimiento",
              },
            },
          ],
          as: "data_horeca",
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
          _id: "$horeca_tipo_negocio",
          total: { $sum: 1 },
        },
      },
      /*{
        $group: {
          _id: "$idOrganizacion",
          total: { $sum: 1},
        },
      },
      {
        $group: {
          _id: "$idUserHoreca",
          total: { $sum: 1},
        },
      },
      {
        $group: {
          _id: {
            idHoreca: "$tipoUsuario",
            tipoNegocio: "$idUserHoreca",
          },
          total: { $sum: 1},
        },
      },*/
      /*
       {
         $group: {
           _id: "$idPunto",
         },
       },
       {
         $lookup: {
           from: "punto_entregas",
           localField: "_id", //Nombre del campo de la coleccion actual
           foreignField: "_id", //Nombre del campo de la coleccion a relacionar
           pipeline: [
             {
               $project: {
                 punto_horeca: "$usuario_horeca",
               },
             },
           ],
           as: "data_punto_entrega", //Nombre del campo donde se insertara todos los documentos relacionados
         },
       },
       {
         $project: {
           horeca_usuario: {
             $arrayElemAt: ["$data_punto_entrega.punto_horeca", 0],
           },
         },
       },
       {
         $lookup: {
           from: "usuario_horecas",
           localField:
             "horeca_usuario",
           foreignField:
             "_id" ,
           pipeline: [
             {
               $project: {
                 horeca_tipo_negocio: "$tipo_negocio",
                 horeca_nombre: "$nombre_establecimiento",
               },
             },
           ],
           as: "data_horeca",
         },
       },
       {
         $project: {
           horeca_usuario: "$horeca_usuario",
           horeca_tipo_negocio: {
             $arrayElemAt: ["$data_horeca.horeca_tipo_negocio", 0],
           },
           horeca_nombre: {
             $arrayElemAt: ["$data_horeca.horeca_nombre", 0],
           },
         },
       },
       {
         $facet: {
           ventas_panaderia: [
             {
               $match: {
                 horeca_tipo_negocio: "PANADERÍA / REPOSTERÍA",
               },
             },
           ],
           ventas_colegios: [
             {
               $match: {
                 horeca_tipo_negocio: "COLEGIO / UNIVERSIDAD / CLUB",
               },
             },
           ],
           ventas_hoteles: [
             {
               $match: {
                 horeca_tipo_negocio: "HOTEL / HOSPITAL",
               },
             },
           ],
           ventas_comedores: [
             {
               $match: {
                 horeca_tipo_negocio: "COMEDOR DE EMPLEADOS",
               },
             },
           ],
           ventas_diversion: [
             {
               $match: {
                 horeca_tipo_negocio: "CENTRO DE DIVERSIÓN",
               },
             },
           ],
           ventas_cafeteria: [
             {
               $match: {
                 horeca_tipo_negocio: "CAFETERÍA / HELADERÍA / SNACK",
               },
             },
           ],
           ventas_catering: [
             {
               $match: {
                 horeca_tipo_negocio: "CATERING SERVICE / SERVICIO ALIMENTACIÓN",
               },
             },
           ],
           ventas_cadena: [
             {
               $match: {
                 horeca_tipo_negocio: "RESTAURANTE DE CADENA",
               },
             },
           ],
           ventas_rapidas: [
             {
               $match: {
                 horeca_tipo_negocio: "COMIDA RÁPIDA",
               },
             },
           ],
           ventas_mayoristas: [
             {
               $match: {
                 horeca_tipo_negocio: "MAYORISTA / MINORISTA",
               },
             },
           ],
           ventas_restaurantes: [
             {
               $match: {
                 horeca_tipo_negocio: "RESTAURANTE",
               },
             },
           ],
         },
       },
       {
         $project: {
           ventas_panaderia: { $size: "$ventas_panaderia" },
           ventas_colegios: { $size: "$ventas_colegios" },
           ventas_hoteles: { $size: "$ventas_hoteles" },
           ventas_comedores: { $size: "$ventas_comedores" },
           ventas_diversion: { $size: "$ventas_diversion" },
           ventas_cafeteria: { $size: "$ventas_cafeteria" },
           ventas_catering: { $size: "$ventas_catering" },
           ventas_cadena: { $size: "$ventas_cadena" },
           ventas_rapidas: { $size: "$ventas_rapidas" },
           ventas_mayoristas: { $size: "$ventas_mayoristas" },
           ventas_restaurantes: { $size: "$ventas_restaurantes" },
         },
       },*/
    ]).exec(callback);
  },
  /** Reporte de categorias de productos que se han vendido de la organizacion */
  categoriasProductoXOrg: function (query, callback) {
    let ObjectId = require("mongoose").Types.ObjectId;
    this.aggregate([
      /** Arma la data de la grafica */
      {
        $match: {
          idOrganizacion: new ObjectId(query),
          $or: [
            { estaActTraking: "Entregado" },
            { estaActTraking: "Recibido" },
            { estaActTraking: "Calificado" },
          ],
        },
      },
      /*{
        $group: {
          _id: "$idPunto",
          total: { $sum: 1},
          idUserHoreca:  { $last: "$idUserHoreca" } ,
        },
      },
      {
        $lookup: {
          from: "usuario_horecas",
          localField:
            "idUserHoreca",
          foreignField:
            "_id" ,
          pipeline: [
            {
              $project: {
                _id: "$_id",
                horeca_tipo_negocio: "$tipo_negocio",
                horeca_nombre: "$nombre_establecimiento",
              },
            },
          ],
          as: "data_horeca",
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
          _id: "$horeca_tipo_negocio",
          total: { $sum: 1},
        },
      },
    */
      {
        $lookup: {
          from: "categoria_productos",
          localField:
            "categoriaProducto",
          foreignField:
            "_id",
          pipeline: [
            {
              $project: {
                _id: "$_id",
                nombre_categoria: "$nombre",
              },
            },
          ],
          as: "dataCat",
        },
      },
      {
        $project: {
          idPunto: '$idPunto',
          categoria: {
            $arrayElemAt: ["$dataCat.nombre_categoria", 0],
          }
        }
      },
      {
        $group: {
          _id: {
            idPunto: "$idPunto",
            cat: "$categoria",
          },
        },
      },
      {
        $group: {
          _id: "$_id.cat",
          count: { $sum: 1 },
        },
      },
      /*{
        $match: {
          idOrganizacion: new ObjectId(query),
        },
      },
      {
        $group: {
          _id: {
            horeca: "$idUserHoreca",
            categoria: "$categoriaProducto"
          }
        }
      },
      {
        $group: {
          _id: "$_id.categoria",
          total: { $sum: 1 }
        }
      },
      {
        $lookup: {
          from: "categoria_productos",
          localField: "_id", //Nombre del campo de la coleccion actual
          foreignField: "_id", //Nombre del campo de la coleccion a relacionar
          pipeline: [
            {
              $project: {
                nombre: "$nombre",
              },
            },
          ],
          as: "data_categoria", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
      {
        $project: {
          _id: { $arrayElemAt: ["$data_categoria.nombre", 0] },
          count: "$total"
        }
      }*/
    ]).exec(callback);
  },
  /** Reporte de sillas de puntos por ciudad que han vendido productos de la organizacion */
  sillasCiudadXOrg: function (query, callback) {
    let ObjectId = require("mongoose").Types.ObjectId;
    this.aggregate([
      /** Arma la data de la grafica */
      {
        $match: {
          $or: [
            { estaActTraking: "Entregado" },
            { estaActTraking: "Recibido" },
            { estaActTraking: "Calificado" },
          ],
        },
      },
      {
        $match: {
          idOrganizacion: new ObjectId(query),
        },
      },
      {
        $group: {
          _id: "$idPunto",
        },
      },
      /** Info del punto de entrega */
      {
        $lookup: {
          from: "punto_entregas",
          localField: "_id", //Nombre del campo de la coleccion actual
          foreignField: "_id", //Nombre del campo de la coleccion a relacionar
          pipeline: [
            {
              $project: {
                punto_ciudad: "$ciudad",
                punto_sillas: "$sillas",
              },
            },
          ],
          as: "data_punto", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
      {
        $project: {
          punto_ciudad: {
            $arrayElemAt: ["$data_punto.punto_ciudad", 0],
          },
          punto_sillas: {
            $arrayElemAt: ["$data_punto.punto_sillas", 0],
          },
        },
      },
      /** Arregla data a mostrar */
      {
        $group: {
          _id: "$punto_ciudad",
          total: { $sum: "$punto_sillas" },
        },
      },
    ]).exec(callback);
  },
  /** Reporte de sillas de tipo de negocio que han vendido productos de la organizacion */
  sillasTipoNegocioXOrg: function (query, callback) {
    let ObjectId = require("mongoose").Types.ObjectId;
    this.aggregate([
      /** Arma la data de la grafica */
      {
        $match: {
          $or: [
            { estaActTraking: "Entregado" },
            { estaActTraking: "Recibido" },
            { estaActTraking: "Calificado" },
          ],
        },
      },
      {
        $match: {
          idOrganizacion: new ObjectId(query),
        },
      },
      {
        $group: {
          _id: "$idPunto",
        },
      },
      /** Punto de entrega */
      {
        $lookup: {
          from: "punto_entregas",
          localField: "_id", //Nombre del campo de la coleccion actual
          foreignField: "_id", //Nombre del campo de la coleccion a relacionar
          pipeline: [
            {
              $project: {
                punto_usuario_horeca: "$usuario_horeca",
                punto_sillas: "$sillas",
              },
            },
          ],
          as: "data_punto", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
      /** Horeca */
      {
        $lookup: {
          from: "usuario_horecas",
          localField:
            "data_punto.punto_usuario_horeca" /** Nombre del campo de la coleccion actual */,
          foreignField:
            "_id" /** Nombre del campo de la coleccion a relacionar */,
          pipeline: [
            {
              $project: {
                horeca_tipo_negocio: "$tipo_negocio",
                horeca_nombre: "$nombre_establecimiento",
              },
            },
          ],
          as: "data_horeca" /** Nombre del campo donde se insertara todos los documentos relacionados */,
        },
      },
      /** Arreglo de datos a mostrar */
      {
        $project: {
          punto_usuario_horeca: {
            $arrayElemAt: ["$data_punto.punto_usuario_horeca", 0],
          },
          punto_sillas: {
            $arrayElemAt: ["$data_punto.punto_sillas", 0],
          },
          horeca_tipo_negocio: {
            $arrayElemAt: ["$data_horeca.horeca_tipo_negocio", 0],
          },
          horeca_nombre: {
            $arrayElemAt: ["$data_horeca.horeca_nombre", 0],
          },
        },
      },
      /** Se agrupan los datos */
      {
        $group: {
          _id: "$horeca_tipo_negocio",
          total: { $sum: "$punto_sillas" },
        },
      },
    ]).exec(callback);
  },
  /** Reporte de sillas por categoria de productos vendidos de la organizacion */
  sillasCategoriaXOrg: function (query, callback) {
    let ObjectId = require("mongoose").Types.ObjectId;
    this.aggregate([
      /** Arma la data de la grafica */
      {
        $match: {
          idOrganizacion: new ObjectId(query),
        },
      },
      {
        $group: {
          _id: "$productoId",
          punto_id: { $first: "$idPunto" },
          categoria_id: { $first: "$categoriaProducto" },
        },
      },
      /** Categoria */
      {
        $lookup: {
          from: "categoria_productos",
          localField: "categoria_id", //Nombre del campo de la coleccion actual
          foreignField: "_id", //Nombre del campo de la coleccion a relacionar
          pipeline: [
            {
              $project: {
                categoria_nombre: "$nombre",
              },
            },
          ],
          as: "data_categoria", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
      /** Punto de entrega */
      {
        $lookup: {
          from: "punto_entregas",
          localField: "punto_id", //Nombre del campo de la coleccion actual
          foreignField: "_id", //Nombre del campo de la coleccion a relacionar
          pipeline: [
            {
              $project: {
                punto_sillas: "$sillas",
              },
            },
          ],
          as: "data_punto", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
      /** Arreglo de datos a mostrar */
      {
        $project: {
          categoria_nombre: {
            $arrayElemAt: ["$data_categoria.categoria_nombre", 0],
          },
          punto_sillas: {
            $arrayElemAt: ["$data_punto.punto_sillas", 0],
          },
          punto_id: "$punto_id",
          categoria_id: "$categoria_id",
        },
      },
      /** Se agrupan los datos */
      {
        $group: {
          _id: {
            categoria_nombre: "$categoria_nombre",
            punto_id: "$punto_id",
            punto_sillas: "$punto_sillas"
          },
          //total: { $sum: "$punto_sillas" },
        },
      },
      {
        $group: {
          _id: "$_id.categoria_nombre",
          total: { $sum: "$_id.punto_sillas" }
        }
      }
    ]).exec(callback);
  },
  /******************** Puntos Feat ********************/
  reportePuntosPorMes: function (query, callback) {
    let ObjectId = require("mongoose").Types.ObjectId;
    this.aggregate([
      {
        $match: {
          idOrganizacion: new ObjectId(query.idOrg),
        },
      },
      // Filtra los puntos si el pedido esta en congelado o el movimiento es congelado
      {
        $lookup: {
          from: "puntos_ganados_establecimientos",
          localField: "idPedido",
          foreignField: "pedido",
          as: "data_valida_puntos",
        },
      },
      {
        $match: {
          $or: [
            { "data_valida_puntos.movimiento": "Aplica" },
            { "data_valida_puntos.movimiento": "Por congelar" },
            { "data_valida_puntos.movimiento": "Congelados feat" },
            { "data_valida_puntos.movimiento": "Congelados" },
            { "data_valida_puntos.movimiento": "CodigoGenerado" },
            { "data_valida_puntos.movimiento": "Redimidos" },
          ],
        },
      },
      /** Data de los pedidos */
      {
        $lookup: {
          from: "pedidos",
          localField: "idPedido",
          foreignField: "_id",
          pipeline: [
            {
              $project: {
                createdAt: "$createdAt",
              },
            },
          ],
          as: "data_pedido",
        },
      },
      {
        $addFields: {
          fecha_pedido: { $arrayElemAt: ["$data_pedido.createdAt", 0] },
        },
      },
      {
        $match: {
          fecha_pedido: {
            $gte: new Date(query.inicio),
            $lte: new Date(query.fin),
          },
        },
      },
      /** agrega la fecha del pedido */
      {
        $addFields: {
          producto_fecha_pedido: {
            $dateToString: {
              format: "%Y-%m",
              date: "$fecha_pedido",
            },
          },
          total_puntos: {
            $multiply: [
              "$unidadesCompradas",
              {
                $arrayElemAt: ["$detalleProducto.precios.puntos_ft_unidad", 0],
              },
            ],
          },
        },
      },
      {
        $group: {
          _id: "$producto_fecha_pedido",
          total_puntos: { $sum: "$total_puntos" },
        },
      },
      {
        $match: {
          total_puntos: { $gt: 0 },
        },
      },
      {
        $sort: { _id: 1 },
      },
    ]).exec(callback);
  },
  reportePuntosPorProducto: function (query, callback) {
    let ObjectId = require("mongoose").Types.ObjectId;
    this.aggregate([
      {
        $match: {
          idOrganizacion: new ObjectId(query.idOrg),
        },
      },
      // Filtra los puntos si el pedido esta en congelado o el movimiento es congelado
      {
        $lookup: {
          from: "puntos_ganados_establecimientos",
          localField: "idPedido",
          foreignField: "pedido",
          as: "data_valida_puntos",
        },
      },
      {
        $match: {
          $or: [
            { "data_valida_puntos.movimiento": "Aplica" },
            { "data_valida_puntos.movimiento": "Por congelar" },
            { "data_valida_puntos.movimiento": "Congelados feat" },
            { "data_valida_puntos.movimiento": "Congelados" },
            { "data_valida_puntos.movimiento": "CodigoGenerado" },
            { "data_valida_puntos.movimiento": "Redimidos" },
          ],
        },
      },
      /** Data de los pedidos */
      {
        $lookup: {
          from: "pedidos",
          localField: "idPedido",
          foreignField: "_id",
          pipeline: [
            {
              $project: {
                createdAt: "$createdAt",
              },
            },
          ],
          as: "data_pedido",
        },
      },
      {
        $addFields: {
          fecha_pedido: { $arrayElemAt: ["$data_pedido.createdAt", 0] },
        },
      },
      {
        $match: {
          fecha_pedido: {
            $gte: new Date(query.inicio),
            $lte: new Date(query.fin),
          },
        },
      },
      {
        $addFields: {
          total_puntos: {
            $multiply: [
              "$unidadesCompradas",
              {
                $arrayElemAt: ["$detalleProducto.precios.puntos_ft_unidad", 0],
              },
            ],
          },
        },
      },
      {
        $group: {
          _id: "$nombreProducto",
          total_puntos: { $sum: "$total_puntos" },
        },
      },
      /** Se agrega este campo para un sort no-case-sensitive */
      {
        $addFields: {
          label_to_sort: {
            $toLower: "$_id",
          },
        },
      },
      {
        $match: {
          total_puntos: { $gt: 0 },
        },
      },
      {
        $sort: { label_to_sort: 1 },
      },
    ]).exec(callback);
  },
  reportePuntosPorCategoria: function (query, callback) {
    let ObjectId = require("mongoose").Types.ObjectId;
    this.aggregate([
      {
        $match: {
          idOrganizacion: new ObjectId(query.idOrg),
        },
      },
      // Filtra los puntos si el pedido esta en congelado o el movimiento es congelado
      {
        $lookup: {
          from: "puntos_ganados_establecimientos",
          localField: "idPedido",
          foreignField: "pedido",
          as: "data_valida_puntos",
        },
      },
      {
        $match: {
          $or: [
            { "data_valida_puntos.movimiento": "Aplica" },
            { "data_valida_puntos.movimiento": "Por congelar" },
            { "data_valida_puntos.movimiento": "Congelados feat" },
            { "data_valida_puntos.movimiento": "Congelados" },
            { "data_valida_puntos.movimiento": "CodigoGenerado" },
            { "data_valida_puntos.movimiento": "Redimidos" },
          ],
        },
      },
      /** Data de los pedidos */
      {
        $lookup: {
          from: "pedidos",
          localField: "idPedido",
          foreignField: "_id",
          pipeline: [
            {
              $project: {
                createdAt: "$createdAt",
              },
            },
          ],
          as: "data_pedido",
        },
      },
      {
        $addFields: {
          fecha_pedido: { $arrayElemAt: ["$data_pedido.createdAt", 0] },
        },
      },
      {
        $match: {
          fecha_pedido: {
            $gte: new Date(query.inicio),
            $lte: new Date(query.fin),
          },
        },
      },
      {
        $lookup: {
          from: "categoria_productos",
          localField: "categoriaProducto",
          foreignField: "_id",
          pipeline: [
            {
              $project: {
                categoria_nombre: "$nombre",
              },
            },
          ],
          as: "data_categoria",
        },
      },
      {
        $addFields: {
          total_puntos: {
            $multiply: [
              "$unidadesCompradas",
              {
                $arrayElemAt: ["$detalleProducto.precios.puntos_ft_unidad", 0],
              },
            ],
          },
          categoria_nombre: {
            $arrayElemAt: ["$data_categoria.categoria_nombre", 0],
          },
        },
      },
      {
        $group: {
          _id: "$categoria_nombre",
          total_puntos: { $sum: "$total_puntos" },
        },
      },
      /** Se agrega este campo para un sort no-case-sensitive */
      {
        $addFields: {
          label_to_sort: {
            $toLower: "$_id",
          },
        },
      },
      {
        $match: {
          total_puntos: { $gt: 0 },
        },
      },
      {
        $sort: { label_to_sort: 1 },
      },
    ]).exec(callback);
  },
  reportePuntosPorMarca: function (query, callback) {
    let ObjectId = require("mongoose").Types.ObjectId;
    this.aggregate([
      {
        $match: {
          idOrganizacion: new ObjectId(query.idOrg),
        },
      },
      // Filtra los puntos si el pedido esta en congelado o el movimiento es congelado
      {
        $lookup: {
          from: "puntos_ganados_establecimientos",
          localField: "idPedido",
          foreignField: "pedido",
          as: "data_valida_puntos",
        },
      },
      {
        $match: {
          $or: [
            { "data_valida_puntos.movimiento": "Aplica" },
            { "data_valida_puntos.movimiento": "Por congelar" },
            { "data_valida_puntos.movimiento": "Congelados feat" },
            { "data_valida_puntos.movimiento": "Congelados" },
            { "data_valida_puntos.movimiento": "CodigoGenerado" },
            { "data_valida_puntos.movimiento": "Redimidos" },
          ],
        },
      },
      /** Data de los pedidos */
      {
        $lookup: {
          from: "pedidos",
          localField: "idPedido",
          foreignField: "_id",
          pipeline: [
            {
              $project: {
                createdAt: "$createdAt",
              },
            },
          ],
          as: "data_pedido",
        },
      },
      {
        $addFields: {
          fecha_pedido: { $arrayElemAt: ["$data_pedido.createdAt", 0] },
        },
      },
      {
        $match: {
          fecha_pedido: {
            $gte: new Date(query.inicio),
            $lte: new Date(query.fin),
          },
        },
      },
      {
        $lookup: {
          from: "marca_productos",
          localField: "marcaProducto",
          foreignField: "_id",
          pipeline: [
            {
              $project: {
                marca_nombre: "$nombre",
              },
            },
          ],
          as: "data_marca",
        },
      },
      {
        $addFields: {
          total_puntos: {
            $multiply: [
              "$unidadesCompradas",
              {
                $arrayElemAt: ["$detalleProducto.precios.puntos_ft_unidad", 0],
              },
            ],
          },
          marca_nombre: {
            $arrayElemAt: ["$data_marca.marca_nombre", 0],
          },
        },
      },
      {
        $group: {
          _id: "$marca_nombre",
          total_puntos: { $sum: "$total_puntos" },
        },
      },
      /** Se agrega este campo para un sort no-case-sensitive */
      {
        $addFields: {
          label_to_sort: {
            $toLower: "$_id",
          },
        },
      },
      {
        $match: {
          total_puntos: { $gt: 0 },
        },
      },
      {
        $sort: { label_to_sort: 1 },
      },
    ]).exec(callback);
  },
  reportePuntosPorDistribuidor: function (query, callback) {
    let ObjectId = require("mongoose").Types.ObjectId;
    this.aggregate([
      {
        $match: {
          idOrganizacion: new ObjectId(query.idOrg),
        },
      },
      // Filtra los puntos si el pedido esta en congelado o el movimiento es congelado
      {
        $lookup: {
          from: "puntos_ganados_establecimientos",
          localField: "idPedido",
          foreignField: "pedido",
          as: "data_valida_puntos",
        },
      },
      {
        $match: {
          $or: [
            { "data_valida_puntos.movimiento": "Aplica" },
            { "data_valida_puntos.movimiento": "Por congelar" },
            { "data_valida_puntos.movimiento": "Congelados feat" },
            { "data_valida_puntos.movimiento": "Congelados" },
            { "data_valida_puntos.movimiento": "CodigoGenerado" },
            { "data_valida_puntos.movimiento": "Redimidos" },
          ],
        },
      },
      /** Data de los pedidos */
      {
        $lookup: {
          from: "pedidos",
          localField: "idPedido",
          foreignField: "_id",
          pipeline: [
            {
              $project: {
                createdAt: "$createdAt",
              },
            },
          ],
          as: "data_pedido",
        },
      },
      {
        $addFields: {
          fecha_pedido: { $arrayElemAt: ["$data_pedido.createdAt", 0] },
        },
      },
      {
        $match: {
          fecha_pedido: {
            $gte: new Date(query.inicio),
            $lte: new Date(query.fin),
          },
        },
      },
      {
        $lookup: {
          from: "distribuidors",
          localField: "idDistribuidor",
          foreignField: "_id",
          pipeline: [
            {
              $project: {
                distribuidor_nombre: "$nombre",
              },
            },
          ],
          as: "data_distribuidor",
        },
      },
      {
        $addFields: {
          total_puntos: {
            $multiply: [
              "$unidadesCompradas",
              {
                $arrayElemAt: ["$detalleProducto.precios.puntos_ft_unidad", 0],
              },
            ],
          },
          distribuidor_nombre: {
            $arrayElemAt: ["$data_distribuidor.distribuidor_nombre", 0],
          },
        },
      },
      {
        $group: {
          _id: "$distribuidor_nombre",
          total_puntos: { $sum: "$total_puntos" },
        },
      },
      /** Se agrega este campo para un sort no-case-sensitive */
      {
        $addFields: {
          label_to_sort: {
            $toLower: "$_id",
          },
        },
      },
      {
        $match: {
          total_puntos: { $gt: 0 },
        },
      },
      {
        $sort: { label_to_sort: 1 },
      },
    ]).exec(callback);
  },
  reportePuntosTop10Productos: function (query, callback) {
    const d = new Date();
    const fecha_referencia = d.setMonth(d.getMonth() - 3);
    let ObjectId = require("mongoose").Types.ObjectId;
    this.aggregate([
      /** Arma la data de la grafica */
      {
        $match: {
          idOrganizacion: new ObjectId(query.idOrg),
          createdAt: {
            $gte: new Date(fecha_referencia),
          },
        },
      },
      // Filtra los puntos si el pedido esta en congelado o el movimiento es congelado
      {
        $lookup: {
          from: "puntos_ganados_establecimientos",
          localField: "idPedido",
          foreignField: "pedido",
          as: "data_valida_puntos",
        },
      },
      {
        $match: {
          $or: [
            { "data_valida_puntos.movimiento": "Aplica" },
            { "data_valida_puntos.movimiento": "Por congelar" },
            { "data_valida_puntos.movimiento": "Congelados feat" },
            { "data_valida_puntos.movimiento": "Congelados" },
            { "data_valida_puntos.movimiento": "CodigoGenerado" },
            { "data_valida_puntos.movimiento": "Redimidos" },
          ],
        },
      },
      {
        $addFields: {
          total_puntos: {
            $multiply: [
              "$unidadesCompradas",
              {
                $arrayElemAt: ["$detalleProducto.precios.puntos_ft_unidad", 0],
              },
            ],
          },
        },
      },
      {
        $group: {
          _id: "$productoId",
          total: { $sum: "$total_puntos" },
          nombre_producto: { $first: "$nombreProducto" },
          codigo_organizacion_producto: {
            $first: "$codigo_organizacion_producto",
          },
        },
      },
      {
        $sort: { total: -1 },
      },
      {
        $project: {
          total: "$total",
          nombre: {
            $concat: [
              "$nombre_producto",
              " (",
              "$codigo_organizacion_producto",
              ")",
            ],
          },
        },
      },
      {
        $match: {
          total: { $gt: 0 },
        },
      },
      {
        $limit: 10,
      },
    ]).exec(callback);
  },
  /******************** Portafolio ********************/
  reporteOrganizacionPortafolioTop10: function (query, callback) {
    const d = new Date();
    const fecha_referencia = d.setMonth(d.getMonth() - 3);
    let ObjectId = require("mongoose").Types.ObjectId;
    this.aggregate([
      /** Arma la data de la grafica */
      {
        $match: {
          idOrganizacion: new ObjectId(query.idOrg),
          createdAt: {
            $gte: new Date(fecha_referencia),
          },
        },
      },
      {
        $addFields: {
          costo_total_venta: {
            $multiply: ["$unidadesCompradas", "$costoProductos"],
          },
        },
      },
      {
        $lookup: {
          from: "pedidos",
          localField: "idPedido", //Nombre del campo de la coleccion actual
          foreignField: "_id", //Nombre del campo de la coleccion a relacionar
          pipeline: [
            {
              $project: {
                estado: "$estado",
              },
            },
          ],
          as: "pedido_estado", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
      {
        $unwind: "$pedido_estado",
      },
      {
        $match: {
          $or: [
            { "pedido_estado.estado": "Entregado" },
            { "pedido_estado.estado": "Recibido" },
            { "pedido_estado.estado": "Calificado" },
          ],
        },
      },
      /** Se agrupan por distribuidor */
      {
        $group: {
          _id: "$productoId",
          total: {
            $sum: "$costo_total_venta",
          },
          nombre_producto: {
            $first: "$nombreProducto",
          },
          foto_producto: {
            $first: {
              $arrayElemAt: ["$detalleProducto.fotos", 0],
            },
          },
        },
      },
      {
        $sort: { total: -1 },
      },
      {
        $limit: 10,
      },
    ]).exec(callback);
  },
  reporteReferenciasPorCategorias: function (query, callback) {
    let ObjectId = require("mongoose").Types.ObjectId;
    this.aggregate([
      {
        $match: {
          idOrganizacion: new ObjectId(query.idOrg),
        },
      },
      /** Data de los pedidos */
      {
        $lookup: {
          from: "pedidos",
          localField: "idPedido",
          foreignField: "_id",
          pipeline: [
            {
              $project: {
                createdAt: "$createdAt",
              },
            },
          ],
          as: "data_pedido",
        },
      },
      {
        $addFields: {
          fecha_pedido: { $arrayElemAt: ["$data_pedido.createdAt", 0] },
        },
      },
      {
        $match: {
          fecha_pedido: {
            $gte: new Date(query.inicio),
            $lte: new Date(query.fin),
          },
        },
      },
      {
        $group: {
          _id: "$productoId",
          categoriaProducto: { $first: "$categoriaProducto" },
        },
      },
      {
        $lookup: {
          from: "categoria_productos",
          localField: "categoriaProducto",
          foreignField: "_id",
          pipeline: [
            {
              $project: {
                categoria_nombre: "$nombre",
              },
            },
          ],
          as: "data_categoria",
        },
      },
      {
        $addFields: {
          categoria_nombre: {
            $arrayElemAt: ["$data_categoria.categoria_nombre", 0],
          },
        },
      },
      {
        $group: {
          _id: "$categoria_nombre",
          total_ventas: { $sum: 1 },
        },
      },
      /** Se agrega este campo para un sort no-case-sensitive */
      {
        $addFields: {
          label_to_sort: {
            $toLower: "$_id",
          },
        },
      },
      {
        $sort: { label_to_sort: 1 },
      },
    ]).exec(callback);
  },
  reporteReferenciasPorMarcas: function (query, callback) {
    let ObjectId = require("mongoose").Types.ObjectId;
    this.aggregate([
      {
        $match: {
          idOrganizacion: new ObjectId(query.idOrg),
        },
      },
      /** Data de los pedidos */
      {
        $lookup: {
          from: "pedidos",
          localField: "idPedido",
          foreignField: "_id",
          pipeline: [
            {
              $project: {
                createdAt: "$createdAt",
              },
            },
          ],
          as: "data_pedido",
        },
      },
      {
        $addFields: {
          fecha_pedido: { $arrayElemAt: ["$data_pedido.createdAt", 0] },
        },
      },
      {
        $match: {
          fecha_pedido: {
            $gte: new Date(query.inicio),
            $lte: new Date(query.fin),
          },
        },
      },
      {
        $lookup: {
          from: "marca_productos",
          localField: "marcaProducto",
          foreignField: "_id",
          pipeline: [
            {
              $project: {
                marca_nombre: "$nombre",
              },
            },
          ],
          as: "data_marca",
        },
      },
      {
        $addFields: {
          marca_nombre: {
            $arrayElemAt: ["$data_marca.marca_nombre", 0],
          },
        },
      },
      {
        $group: {
          _id: "$marca_nombre",
          total_ventas: { $sum: 1 },
        },
      },
      /** Se agrega este campo para un sort no-case-sensitive */
      {
        $addFields: {
          label_to_sort: {
            $toLower: "$_id",
          },
        },
      },
      {
        $sort: { label_to_sort: 1 },
      },
    ]).exec(callback);
  },
  reporteVentasPorCategorias: function (query, callback) {
    let ObjectId = require("mongoose").Types.ObjectId;
    this.aggregate([
      {
        $match: {
          idOrganizacion: new ObjectId(query.idOrg),
        },
      },
      /** Data de los pedidos */
      {
        $lookup: {
          from: "pedidos",
          localField: "idPedido",
          foreignField: "_id",
          pipeline: [
            {
              $project: {
                createdAt: "$createdAt",
              },
            },
          ],
          as: "data_pedido",
        },
      },
      {
        $addFields: {
          fecha_pedido: { $arrayElemAt: ["$data_pedido.createdAt", 0] },
        },
      },
      {
        $match: {
          fecha_pedido: {
            $gte: new Date(query.inicio),
            $lte: new Date(query.fin),
          },
        },
      },
      {
        $lookup: {
          from: "categoria_productos",
          localField: "categoriaProducto",
          foreignField: "_id",
          pipeline: [
            {
              $project: {
                categoria_nombre: "$nombre",
              },
            },
          ],
          as: "data_categoria",
        },
      },
      {
        $addFields: {
          total_ventas: {
            $multiply: [
              "$unidadesCompradas",
              {
                $arrayElemAt: ["$detalleProducto.precios.precio_unidad", 0],
              },
            ],
          },
          categoria_nombre: {
            $arrayElemAt: ["$data_categoria.categoria_nombre", 0],
          },
        },
      },
      {
        $group: {
          _id: "$categoria_nombre",
          total_ventas: { $sum: "$total_ventas" },
        },
      },
      /** Se agrega este campo para un sort no-case-sensitive */
      {
        $addFields: {
          label_to_sort: {
            $toLower: "$_id",
          },
        },
      },
      {
        $sort: { label_to_sort: 1 },
      },
    ]).exec(callback);
  },
  reporteVentasPorMarcas: function (query, callback) {
    let ObjectId = require("mongoose").Types.ObjectId;
    this.aggregate([
      {
        $match: {
          idOrganizacion: new ObjectId(query.idOrg),
        },
      },
      /** Data de los pedidos */
      {
        $lookup: {
          from: "pedidos",
          localField: "idPedido",
          foreignField: "_id",
          pipeline: [
            {
              $project: {
                createdAt: "$createdAt",
              },
            },
          ],
          as: "data_pedido",
        },
      },
      {
        $addFields: {
          fecha_pedido: { $arrayElemAt: ["$data_pedido.createdAt", 0] },
        },
      },
      {
        $match: {
          fecha_pedido: {
            $gte: new Date(query.inicio),
            $lte: new Date(query.fin),
          },
        },
      },
      {
        $lookup: {
          from: "marca_productos",
          localField: "marcaProducto",
          foreignField: "_id",
          pipeline: [
            {
              $project: {
                marca_nombre: "$nombre",
              },
            },
          ],
          as: "data_marca",
        },
      },
      {
        $addFields: {
          total_ventas: {
            $multiply: [
              "$unidadesCompradas",
              {
                $arrayElemAt: ["$detalleProducto.precios.precio_unidad", 0],
              },
            ],
          },
          marca_nombre: {
            $arrayElemAt: ["$data_marca.marca_nombre", 0],
          },
        },
      },
      {
        $group: {
          _id: "$marca_nombre",
          total_ventas: { $sum: "$total_ventas" },
        },
      },
      /** Se agrega este campo para un sort no-case-sensitive */
      {
        $addFields: {
          label_to_sort: {
            $toLower: "$_id",
          },
        },
      },
      {
        $sort: { label_to_sort: 1 },
      },
    ]).exec(callback);
  },
  /******************************** Distribuidor *******************************/
  getOrgGrafDistribuidorVentas: function (query, callback) {
    const d = new Date();
    const fecha_referencia = d.setMonth(d.getMonth() - 3);
    let ObjectId = require("mongoose").Types.ObjectId;
    this.aggregate([
      /** Arma la data de la grafica */
      {
        $match: {
          idOrganizacion: new ObjectId(query.idOrg),
        },
      },
      /** Data de los pedidos y se filtra ultimos 3 meses */
      {
        $lookup: {
          from: "pedidos",
          localField: "idPedido",
          foreignField: "_id",
          pipeline: [
            {
              $project: {
                createdAt: "$createdAt",
                estado: "$estado",
              },
            },
          ],
          as: "data_pedido",
        },
      },
      // Filtra las ventas despues de recibido
      {
        $match: {
          $or: [
            { "data_pedido.estado": "Entregado" },
            { "data_pedido.estado": "Recibido" },
            { "data_pedido.estado": "Calificado" },
          ],
        },
      },
      {
        $addFields: {
          fecha_pedido: { $arrayElemAt: ["$data_pedido.createdAt", 0] },
        },
      },
      {
        $match: {
          createdAt: {
            $gte: new Date(fecha_referencia),
          },
        },
      },
      /** Se calcula costo de venta */
      {
        $addFields: {
          costo_total_venta: {
            $multiply: ["$unidadesCompradas", "$costoProductos"],
          },
        },
      },
      /** Se agrupan por distribuidor */
      {
        $group: {
          _id: "$idDistribuidor",
          total: {
            $sum: "$costo_total_venta",
          },
        },
      },
      /** Info distribuidor */
      {
        $lookup: {
          from: "distribuidors",
          localField: "_id", //Nombre del campo de la coleccion actual
          foreignField: "_id", //Nombre del campo de la coleccion a relacionar
          pipeline: [
            {
              $project: {
                nombre: "$nombre",
                ciudad: "$ciudad",
              },
            },
          ],
          as: "data_distribuidor", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
      /** Arma la data a mostrar */
      {
        $project: {
          distribuidor_nombre: {
            $arrayElemAt: ["$data_distribuidor.nombre", 0],
          },
          distribuidor_ciudad: {
            $arrayElemAt: ["$data_distribuidor.ciudad", 0],
          },
          total: "$total",
        },
      },
      {
        $sort: { total: -1 },
      },
    ]).exec(callback);
  },
  getOrgGrafDistribuidorCiudades: function (query, callback) {
    let ObjectId = require("mongoose").Types.ObjectId;
    this.aggregate([
      /** Arma la data de la grafica */
      {
        $match: {
          idOrganizacion: new ObjectId(query.idOrg),
        },
      },
      // Filtra las ventas despues de recibido
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
          $or: [
            { "data_pedido.estado": "Entregado" },
            { "data_pedido.estado": "Recibido" },
            { "data_pedido.estado": "Calificado" },
          ],
        },
      },
      /** Se agrupan por distribuidor */
      {
        $group: {
          _id: "$idDistribuidor",
        },
      },
      /** Info distribuidor */
      {
        $lookup: {
          from: "distribuidors",
          localField: "_id", //Nombre del campo de la coleccion actual
          foreignField: "_id", //Nombre del campo de la coleccion a relacionar
          pipeline: [
            {
              $project: {
                ciudad: "$ciudad",
                nombre: "$nombre",
              },
            },
          ],
          as: "data_punto", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
      /** Arma la data a mostrar */
      {
        $project: {
          punto_ciudad: {
            $arrayElemAt: ["$data_punto.ciudad", 0],
          },
        },
      },
      {
        $group: {
          _id: "$punto_ciudad",
        },
      },
      /** Se agrega este campo para un sort no-case-sensitive */
      {
        $addFields: {
          label_to_sort: {
            $toLower: "$_id",
          },
        },
      },
      {
        $sort: { label_to_sort: 1 },
      },
    ]).exec(callback);
  },
  getOrgGrafDistribuidorMes: function (query, callback) {
    let ObjectId = require("mongoose").Types.ObjectId;
    this.aggregate([
      /** Arma la data de la grafica */
      {
        $match: {
          idOrganizacion: new ObjectId(query.idOrg),
        },
      },
      /** Data de los pedidos y se agrega campo mes-año formateado */
      {
        $lookup: {
          from: "pedidos",
          localField: "idPedido",
          foreignField: "_id",
          pipeline: [
            {
              $project: {
                estado: "$estado",
                createdAt: "$createdAt",
              },
            },
          ],
          as: "data_pedido",
        },
      },
      // Filtra las ventas despues de recibido
      {
        $match: {
          $or: [
            { "data_pedido.estado": "Entregado" },
            { "data_pedido.estado": "Recibido" },
            { "data_pedido.estado": "Calificado" },
          ],
        },
      },
      /** Info distribuidor */
      {
        $lookup: {
          from: "distribuidors",
          localField: "idDistribuidor", //Nombre del campo de la coleccion actual
          foreignField: "_id", //Nombre del campo de la coleccion a relacionar
          pipeline: [
            {
              $project: {
                nombre: "$nombre",
              },
            },
          ],
          as: "data_distribuidor", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
      {
        $addFields: {
          fecha_pedido: { $arrayElemAt: ["$data_pedido.createdAt", 0] },
        },
      },
      {
        $addFields: {
          /** Se cambia formato a la fecha */
          fechaGroup: {
            $dateToString: {
              format: "%Y-%m",
              date: "$fecha_pedido",
            },
          },
          /** Se calcula costo de venta */
          costo_total_venta: {
            $multiply: ["$unidadesCompradas", "$costoProductos"],
          },
          /** Se agrega el nombre del dist como string fuera del objeto */
          distribuidor_nombre: {
            $arrayElemAt: ["$data_distribuidor.nombre", 0],
          },
        },
      },
      /** Se agrupan por distribuidor y fecah formateada */
      {
        $group: {
          _id: {
            fechaGroup: "$fechaGroup",
            distribuidor_nombre: "$distribuidor_nombre",
          },
          total: {
            $sum: "$costo_total_venta",
          },
          fechaGroup: {
            $first: "$fechaGroup",
          },
          distribuidor_nombre: {
            $first: "$distribuidor_nombre",
          },
        },
      },
      {
        $sort: { total: -1 },
      },
    ]).exec(callback);
  },
  getOrgGrafDistribuidorPuntosAlcanzados: function (query, callback) {
    let ObjectId = require("mongoose").Types.ObjectId;
    this.aggregate([
      /** Arma la data de la grafica */
      {
        $match: {
          idOrganizacion: new ObjectId(query.idOrg),
        },
      },
      // Filtra las ventas despues de recibido
      {
        $lookup: {
          from: "pedidos",
          localField: "idPedido",
          foreignField: "_id",
          as: "data_pedido",
        },
      },
      {
        $match: {
          $or: [
            { "data_pedido.estado": "Entregado" },
            { "data_pedido.estado": "Recibido" },
            { "data_pedido.estado": "Calificado" },
          ],
        },
      },
      /** Info distribuidor */
      {
        $lookup: {
          from: "distribuidors",
          localField: "idDistribuidor", //Nombre del campo de la coleccion actual
          foreignField: "_id", //Nombre del campo de la coleccion a relacionar
          pipeline: [
            {
              $project: {
                nombre: "$nombre",
              },
            },
          ],
          as: "data_distribuidor", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
      {
        $addFields: {
          fecha_pedido: { $arrayElemAt: ["$data_pedido.createdAt", 0] },
        },
      },
      {
        $addFields: {
          /** Se agrega el nombre del dist como string fuera del objeto */
          distribuidor_nombre: {
            $arrayElemAt: ["$data_distribuidor.nombre", 0],
          },
        },
      },
      /** Se agrupan por distribuidor y fecah formateada */
      {
        $group: {
          _id: {
            punto_id: "$idPunto",
            distribuidor_nombre: "$distribuidor_nombre",
          },
          total: { $sum: 1 },
        },
      },
      {
        $group: {
          _id: "$_id.distribuidor_nombre",
          total: { $sum: 1 },
        },
      },
      {
        $sort: { total: -1 },
      },
    ]).exec(callback);
  },
  getOrgGrafDistribuidorPorCiudad: function (query, callback) {
    let ObjectId = require("mongoose").Types.ObjectId;
    this.aggregate([
      /** Arma la data de la grafica */
      {
        $match: {
          idOrganizacion: new ObjectId(query.idOrg),
        },
      },
      // Filtra las ventas despues de recibido
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
          $or: [
            { "data_pedido.estado": "Entregado" },
            { "data_pedido.estado": "Recibido" },
            { "data_pedido.estado": "Calificado" },
          ],
        },
      },
      /** Info distribuidor */
      {
        $lookup: {
          from: "distribuidors",
          localField: "idDistribuidor", //Nombre del campo de la coleccion actual
          foreignField: "_id", //Nombre del campo de la coleccion a relacionar
          pipeline: [
            {
              $project: {
                ciudad: "$ciudad",
              },
            },
          ],
          as: "data_distribuidor", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
      {
        $addFields: {
          distribuidor_ciudad: {
            $arrayElemAt: ["$data_distribuidor.ciudad", 0],
          },
        },
      },
      {
        $group: {
          _id: "$idDistribuidor",
          distribuidor_ciudad: { $first: "$distribuidor_ciudad" },
        },
      },
      {
        $group: {
          _id: "$distribuidor_ciudad",
          total: { $sum: 1 },
        },
      },
      {
        $sort: { total: -1 },
      },
    ]).exec(callback);
  },
  getOrgGrafDistribuidorMesesArray: function (query, callback) {
    let ObjectId = require("mongoose").Types.ObjectId;
    this.aggregate([
      /** Arma la data de la grafica */
      {
        $match: {
          idOrganizacion: new ObjectId(query.idOrg),
        },
      },
      /** Data de los pedidos y se agrega campo mes-año formateado */
      {
        $lookup: {
          from: "pedidos",
          localField: "idPedido",
          foreignField: "_id",
          pipeline: [
            {
              $project: {
                createdAt: "$createdAt",
              },
            },
          ],
          as: "data_pedido",
        },
      },
      {
        $addFields: {
          fecha_pedido: { $arrayElemAt: ["$data_pedido.createdAt", 0] },
        },
      },
      {
        $addFields: {
          /** Se cambia formato a la fecha */
          fechaGroup: {
            $dateToString: {
              format: "%Y-%m",
              date: "$fecha_pedido",
            },
          },
        },
      },
      /** Se agrupan por distribuidor y fecah formateada */
      {
        $group: {
          _id: "$fechaGroup",
          total: { $sum: 1 },
        },
      },
      {
        $sort: { _id: 1 },
      },
    ]).exec(callback);
  },
  getOrgGrafDistribuidorPuntos: function (query, callback) {
    const d = new Date();
    const fecha_referencia = d.setMonth(d.getMonth() - 3);
    let ObjectId = require("mongoose").Types.ObjectId;
    this.aggregate([
      /** Arma la data de la grafica */
      {
        $match: {
          idOrganizacion: new ObjectId(query.idOrg),
        },
      },
      /** Data de los pedidos y se filtra ultimos 3 meses */
      {
        $lookup: {
          from: "pedidos",
          localField: "idPedido",
          foreignField: "_id",
          pipeline: [
            {
              $project: {
                createdAt: "$createdAt",
              },
            },
          ],
          as: "data_pedido",
        },
      },
      {
        $addFields: {
          fecha_pedido: { $arrayElemAt: ["$data_pedido.createdAt", 0] },
        },
      },
      {
        $match: {
          createdAt: {
            $gte: new Date(fecha_referencia),
          },
        },
      },
      /** Se agrega campo de fecha con formato */
      {
        $addFields: {
          /** Se cambia formato a la fecha */
          fechaGroup: {
            $dateToString: {
              format: "%Y-%m",
              date: "$fecha_pedido",
            },
          },
        },
      },
      /** Se agrupan por punto y fecha para evitar que en un mes se repita el punto */
      {
        $group: {
          _id: {
            idPunto: "$idPunto",
            fecha_pedido: "$fechaGroup",
          },
          fechaGroup: { $first: "$fechaGroup" },
        },
      },
      /** Se agrupan por fecha de pedido y se suma cada coincidencia, esto me dará los puntos por mes */
      {
        $group: {
          _id: "$fechaGroup",
          total: { $sum: 1 },
        },
      },
      {
        $sort: { fecha_pedido: 1 },
      },
    ]).exec(callback);
  },
  getOrgGrafDistribuidorVendedores: function (query, callback) {
    let ObjectId = require("mongoose").Types.ObjectId;
    this.aggregate([
      {
        $match: {
          idOrganizacion: new ObjectId(query.idOrg),
        },
      },
      // Filtra las ventas despues de recibido
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
          $or: [
            { "data_pedido.estado": "Entregado" },
            { "data_pedido.estado": "Recibido" },
            { "data_pedido.estado": "Calificado" },
          ],
        },
      },
      /** Se agrupan por distribuidor y punto para evitar duplicidad*/
      {
        $group: {
          _id: {
            idPunto: "$idPunto",
            idDistribuidor: "$idDistribuidor",
          },
          idPunto: { $first: "$idPunto" },
          idDistribuidor: { $first: "$idDistribuidor" },
        },
      },
      {
        /** Revisamos la vinculacion entre punto y distribuidor, especificamente los vendedores buscando la cantidad de vendedores asignados entre dist y punto */
        $lookup: {
          from: "distribuidores_vinculados",
          localField: "idPunto",
          foreignField: "punto_entrega",
          as: "data_vinculacion",
        },
      },
      {
        $lookup: {
          from: "distribuidores_vinculados",
          localField: "idDistribuidor",
          foreignField: "distribuidor",
          pipeline: [
            {
              $addFields: {
                total_vendedores: { $size: "$vendedor" },
              },
            },
          ],
          as: "data_vinculacion_final",
        },
      },
      /** se agrega el nombre del dist */
      {
        $lookup: {
          from: "distribuidors",
          localField: "idDistribuidor", //Nombre del campo de la coleccion actual
          foreignField: "_id", //Nombre del campo de la coleccion a relacionar
          pipeline: [
            {
              $project: {
                nombre: "$nombre",
              },
            },
          ],
          as: "data_distribuidor", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
      /** Se organiza solo data a mostrar */
      {
        $project: {
          distribuidor_nombre: {
            $arrayElemAt: ["$data_distribuidor.nombre", 0],
          },
          total_vendedores: {
            $arrayElemAt: ["$data_vinculacion_final.total_vendedores", 0],
          },
        },
      },
      /** Se agrupan por distribuidor y se dejan descendetes */
      {
        $group: {
          _id: "$distribuidor_nombre",
          total: { $sum: "$total_vendedores" },
        },
      },
      {
        $sort: { total: -1 },
      },
    ]).exec(callback);
  },
  /**  Ventas por categoría de producto */
  getInformeDistribuidorPortafolioVentasXCat: function (query, callback) {
    let ObjectId = require("mongoose").Types.ObjectId;
    this.aggregate([
      {
        $match: {
          idDistribuidor: new ObjectId(query.idDistribuidor),
          createdAt: {
            $gte: new Date(query.inicio),
            $lte: new Date(query.fin),
          },
        },
      },

      /** Arma la data de la grafica */
      {
        $addFields: {
          costoTotalProducto: {
            $multiply: ["$unidadesCompradas", "$costoProductos"],
          },
        },
      },
      // Filtra las ventas despues de recibido
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
          $or: [
            { "data_pedido.estado": "Entregado" },
            { "data_pedido.estado": "Recibido" },
            { "data_pedido.estado": "Calificado" },
          ],
        },
      },
      {
        $group: {
          _id: "$categoriaProducto",
          total: { $sum: "$costoTotalProducto" },
        },
      },
      {
        $lookup: {
          from: "categoria_productos", //Nombre de la colecccion a relacionar
          localField: "_id", //Nombre del campo de la coleccion actual
          foreignField: "_id", //Nombre del campo de la coleccion a relacionar
          as: "data_categoria", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
      /** Se organiza solo data a mostrar */
      {
        $project: {
          _id: {
            $arrayElemAt: ["$data_categoria.nombre", 0],
          },
          total: "$total",
        },
      },
      {
        $sort: { total: -1 },
      },
    ]).exec(callback);
  },
  /**  Ventas por organizacion */
  getInformeDistribuidorPortafolioVentasXOrg: function (query, callback) {
    let ObjectId = require("mongoose").Types.ObjectId;
    this.aggregate([
      {
        $match: {
          idDistribuidor: new ObjectId(query.idDistribuidor),
          createdAt: {
            $gte: new Date(query.inicio),
            $lte: new Date(query.fin),
          },
        },
      },

      // Arma la data de la grafica
      {
        $addFields: {
          costoTotalProducto: {
            $multiply: ["$unidadesCompradas", "$costoProductos"],
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
          $or: [
            { "data_pedido.estado": "Entregado" },
            { "data_pedido.estado": "Recibido" },
            { "data_pedido.estado": "Calificado" },
          ],
        },
      },
      {
        $group: {
          _id: "$idOrganizacion",
          total: { $sum: "$costoTotalProducto" },
        },
      },
      {
        $lookup: {
          from: "organizacions", //Nombre de la colecccion a relacionar
          localField: "_id", //Nombre del campo de la coleccion actual
          foreignField: "_id", //Nombre del campo de la coleccion a relacionar
          as: "data_organizacion", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
      // Se organiza solo data a mostrar
      {
        $project: {
          _id: {
            $arrayElemAt: ["$data_organizacion.nombre", 0],
          },
          total: "$total",
        },
      },
      {
        $sort: { total: -1 },
      },
    ]).exec(callback);
  },
  /**  Top 10 productos_ciudad */
  getInformeDistribuidorPortafolioTop10Prod: function (query, callback) {
    let ObjectId = require("mongoose").Types.ObjectId;
    this.aggregate([
      {
        $match: {
          idDistribuidor: new ObjectId(query.idDistribuidor),
          createdAt: {
            $gte: new Date(query.inicio),
            $lte: new Date(query.fin),
          },
        },
      },
      {
        $match: {
          $or: [
            { estaActTraking: "Entregado" },
            { estaActTraking: "Recibido" },
            { estaActTraking: "Calificado" },
          ],
        },
      },
      /** Arma la data de la grafica */
      {
        $addFields: {
          costoTotalProducto: {
            $multiply: ["$unidadesCompradas", "$costoProductos"],
          },
        },
      },
      {
        $group: {
          _id: "$nombreProducto",
          total: { $sum: "$costoTotalProducto" },
          cod_prod: { $first: "$codigoDistribuidorProducto" },
        },
      },
      {
        $sort: { total: -1 },
      },
      {
        $limit: 10,
      },
    ]).exec(callback);
  },
  /** Pedidos por tipo de negocio */
  getInformeDistribuidorPedidosTipoNegocio: function (query, callback) {
    let ObjectId = require("mongoose").Types.ObjectId;
    this.aggregate([
      {
        $match: {
          idDistribuidor: new ObjectId(query.idDistribuidor),
          createdAt: {
            $gte: new Date(query.inicio),
            $lte: new Date(query.fin),
          },
        },
      },
      /** Depura los punto/pedidos repetidos */
      {
        $group: {
          _id: "$idPedido",
          idPunto: { $first: "$idPunto" },
        },
      },
      /** Se recupera la info de cada punto */
      {
        $lookup: {
          from: "punto_entregas",
          localField: "idPunto" /** Nombre del campo de la coleccion actual */,
          foreignField:
            "_id" /** Nombre del campo de la coleccion a relacionar */,
          as: "dataPunto" /** Nombre del campo donde se insertara todos los documentos relacionados */,
        },
      },
      {
        $lookup: {
          from: "usuario_horecas",
          localField:
            "dataPunto.usuario_horeca" /** Nombre del campo de la coleccion actual */,
          foreignField:
            "_id" /** Nombre del campo de la coleccion a relacionar */,
          as: "dataHoreca" /** Nombre del campo donde se insertara todos los documentos relacionados */,
        },
      },
      {
        $addFields: {
          dataHoreca: { $arrayElemAt: ["$dataHoreca", 0] },
        },
      },
      {
        $project: {
          tipo_negocio: "$dataHoreca.tipo_negocio",
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
  /** Ventas distribuidor por mes */
  getInformeDistribuidorVentasMes: function (query, callback) {
    let ObjectId = require("mongoose").Types.ObjectId;
    this.aggregate([
      {
        $match: {
          idDistribuidor: new ObjectId(query.idDistribuidor),
          createdAt: {
            $gte: new Date(query.inicio),
            $lte: new Date(query.fin),
          },
        },
      },
      /** agrega la fecha del pedido y el vaor comprado*/
      {
        $addFields: {
          producto_fecha_pedido: {
            $dateToString: {
              format: "%Y-%m",
              date: "$createdAt",
            },
          },
          costoTotalProducto: {
            $multiply: ["$unidadesCompradas", "$costoProductos"],
          },
        },
      },
      // Filtra las ventas despues de recibido
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
          $or: [
            { "data_pedido.estado": "Entregado" },
            { "data_pedido.estado": "Recibido" },
            { "data_pedido.estado": "Calificado" },
          ],
        },
      },
      {
        $group: {
          _id: "$producto_fecha_pedido",
          total: { $sum: "$costoTotalProducto" },
        },
      },
      {
        $sort: { _id: 1 },
      },
    ]).exec(callback);
  },
  /** Ventas por tipo de negocio */
  getInformeDistribuidorVentasTipoNegocio: function (query, callback) {
    let ObjectId = require("mongoose").Types.ObjectId;
    this.aggregate([
      {
        $match: {
          idDistribuidor: new ObjectId(query.idDistribuidor),
          createdAt: {
            $gte: new Date(query.inicio),
            $lte: new Date(query.fin),
          },
        },
      },
      /** agrega el vaor comprado */
      {
        $addFields: {
          costoTotalProducto: {
            $multiply: ["$unidadesCompradas", "$costoProductos"],
          },
        },
      },
      /** Se recupera la info de cada punto */
      {
        $lookup: {
          from: "punto_entregas",
          localField: "idPunto" /** Nombre del campo de la coleccion actual */,
          foreignField:
            "_id" /** Nombre del campo de la coleccion a relacionar */,
          as: "dataPunto" /** Nombre del campo donde se insertara todos los documentos relacionados */,
        },
      },
      {
        $lookup: {
          from: "usuario_horecas",
          localField:
            "dataPunto.usuario_horeca" /** Nombre del campo de la coleccion actual */,
          foreignField:
            "_id" /** Nombre del campo de la coleccion a relacionar */,
          as: "dataHoreca" /** Nombre del campo donde se insertara todos los documentos relacionados */,
        },
      },
      {
        $addFields: {
          dataHoreca: { $arrayElemAt: ["$dataHoreca", 0] },
        },
      },
      // Filtra las ventas despues de recibido
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
          $or: [
            { "data_pedido.estado": "Entregado" },
            { "data_pedido.estado": "Recibido" },
            { "data_pedido.estado": "Calificado" },
          ],
        },
      },
      {
        $project: {
          tipo_negocio: "$dataHoreca.tipo_negocio",
          costoTotalProducto: "$costoTotalProducto",
        },
      },
      {
        $group: {
          _id: "$tipo_negocio",
          total: { $sum: "$costoTotalProducto" },
        },
      },
      {
        $sort: { total: -1 },
      },
    ]).exec(callback);
  },
  /**  Ventas por marca de producto */
  getInformeDistribuidorVentasMarca: function (query, callback) {
    let ObjectId = require("mongoose").Types.ObjectId;
    this.aggregate([
      {
        $match: {
          idDistribuidor: new ObjectId(query.idDistribuidor),
          createdAt: {
            $gte: new Date(query.inicio),
            $lte: new Date(query.fin),
          },
        },
      },
      // Filtra las ventas despues de recibido
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
          $or: [
            { "data_pedido.estado": "Entregado" },
            { "data_pedido.estado": "Recibido" },
            { "data_pedido.estado": "Calificado" },
          ],
        },
      },
      /** Arma la data de la grafica */
      {
        $addFields: {
          costoTotalProducto: {
            $multiply: ["$unidadesCompradas", "$costoProductos"],
          },
        },
      },
      {
        $group: {
          _id: "$marcaProducto",
          total: { $sum: "$costoTotalProducto" },
        },
      },
      {
        $lookup: {
          from: "marca_productos", //Nombre de la colecccion a relacionar
          localField: "_id", //Nombre del campo de la coleccion actual
          foreignField: "_id", //Nombre del campo de la coleccion a relacionar
          as: "data_marca", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
      /** Se organiza solo data a mostrar */
      {
        $project: {
          _id: {
            $arrayElemAt: ["$data_marca.nombre", 0],
          },
          total: "$total",
        },
      },
      {
        $sort: { total: -1 },
      },
    ]).exec(callback);
  },
  /**  Ventas por usuario vendedor*/
  getInformeDistribuidorVentasVendedor: function (query, callback) {
    let ObjectId = require("mongoose").Types.ObjectId;
    this.aggregate([
      {
        $match: {
          idDistribuidor: new ObjectId(query.idDistribuidor),
          createdAt: {
            $gte: new Date(query.inicio),
            $lte: new Date(query.fin),
          },
        },
      },
      // Filtra las ventas despues de recibido
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
          $or: [
            { "data_pedido.estado": "Entregado" },
            { "data_pedido.estado": "Recibido" },
            { "data_pedido.estado": "Calificado" },
          ],
        },
      },
      /** Arma la data de la grafica */
      {
        $addFields: {
          costoTotalProducto: {
            $multiply: ["$unidadesCompradas", "$costoProductos"],
          },
        },
      },
      {
        $lookup: {
          from: "distribuidores_vinculados",
          let: {
            punto: "$idPunto",
            distribuidor: "$idDistribuidor",
          },
          pipeline: [
            /** Retorna solo las compras entre punto y distribuidor */
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
              $unwind: "$vendedor",
            },
            {
              $lookup: {
                from: "trabajadors",
                localField: "vendedor", //Nombre del campo de la coleccion actual
                foreignField: "_id", //Nombre del campo de la coleccion a relacionar
                pipeline: [
                  {
                    $project: {
                      nombre: { $concat: ["$nombres", " ", "$apellidos"] },
                    },
                  },
                ],
                as: "data_trabajadores", //Nombre del campo donde se insertara todos los documentos relacionados
              },
            },
            {
              $addFields: {
                data_trabajador: {
                  $arrayElemAt: ["$data_trabajadores.nombre", 0],
                },
              },
            },
          ],
          as: "data_distribuidores_vinculados", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
      {
        $addFields: {
          "data_distribuidores_vinculados.costoTotalProducto": {
            $multiply: ["$unidadesCompradas", "$costoProductos"],
          },
        },
      },
      {
        $unwind: "$data_distribuidores_vinculados",
      },
      {
        $replaceRoot: {
          newRoot: "$data_distribuidores_vinculados",
        },
      },
      {
        $group: {
          _id: "$data_trabajador",
          total: { $sum: "$costoTotalProducto" },
        },
      },
      {
        $sort: { total: -1 },
      },
    ]).exec(callback);
  },
  /** Ventas por ciudad */
  getInformeDistribuidorVentasCiudad: function (query, callback) {
    let ObjectId = require("mongoose").Types.ObjectId;
    this.aggregate([
      {
        $match: {
          idDistribuidor: new ObjectId(query.idDistribuidor),
          createdAt: {
            $gte: new Date(query.inicio),
            $lte: new Date(query.fin),
          },
        },
      },
      // Filtra las ventas despues de recibido
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
          $or: [
            { "data_pedido.estado": "Entregado" },
            { "data_pedido.estado": "Recibido" },
            { "data_pedido.estado": "Calificado" },
          ],
        },
      },
      /** agrega el valor comprado */
      {
        $addFields: {
          costoTotalProducto: {
            $multiply: ["$unidadesCompradas", "$costoProductos"],
          },
        },
      },
      /** Se recupera la info de cada punto */
      {
        $lookup: {
          from: "punto_entregas",
          localField: "idPunto" /** Nombre del campo de la coleccion actual */,
          foreignField:
            "_id" /** Nombre del campo de la coleccion a relacionar */,
          as: "data_punto" /** Nombre del campo donde se insertara todos los documentos relacionados */,
        },
      },
      {
        $addFields: {
          punto_ciudad: { $arrayElemAt: ["$data_punto.ciudad", 0] },
        },
      },
      {
        $group: {
          _id: "$punto_ciudad",
          total: { $sum: "$costoTotalProducto" },
        },
      },
      {
        $sort: { total: -1 },
      },
    ]).exec(callback);
  },
  /** Ventas productos distribuidor por mes */
  getInformeDistribuidorVentasProductosMes: function (query, callback) {
    let ObjectId = require("mongoose").Types.ObjectId;
    this.aggregate([
      {
        $match: {
          idDistribuidor: new ObjectId(query.idDistribuidor),
          createdAt: {
            $gte: new Date(query.inicio),
            $lte: new Date(query.fin),
          },
        },
      },
      // Filtra las ventas despues de recibido
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
          $or: [
            { "data_pedido.estado": "Entregado" },
            { "data_pedido.estado": "Recibido" },
            { "data_pedido.estado": "Calificado" },
          ],
        },
      },
      /** agrega la fecha del pedido y el vaor comprado*/
      {
        $addFields: {
          producto_fecha_pedido: {
            $dateToString: {
              format: "%Y-%m",
              date: "$createdAt",
            },
          },
          costoTotalProducto: {
            $multiply: ["$unidadesCompradas", "$costoProductos"],
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
  /** Ventas productos distribuidor por sillas alcanzadas */
  getInformeDistribuidorVentasSillas: function (query, callback) {
    let ObjectId = require("mongoose").Types.ObjectId;
    this.aggregate([
      {
        $match: {
          idDistribuidor: new ObjectId(query.idDistribuidor),
          createdAt: {
            $gte: new Date(query.inicio),
            $lte: new Date(query.fin),
          },
        },
      },
      {
        $addFields: {
          costoTotalProducto: {
            $multiply: ["$unidadesCompradas", "$costoProductos"],
          },
        },
      },
      // Filtra las ventas despues de recibido
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
          $or: [
            { "data_pedido.estado": "Entregado" },
            { "data_pedido.estado": "Recibido" },
            { "data_pedido.estado": "Calificado" },
          ],
        },
      },
      {
        $facet: {
          sillas501: [
            {
              $match: {
                puntoSillas: {
                  $gte: 501,
                },
              },
            },
          ],
          sillas301_500: [
            {
              $match: {
                puntoSillas: {
                  $gte: 301,
                  $lte: 500,
                },
              },
            },
          ],
          sillas151_300: [
            {
              $match: {
                puntoSillas: {
                  $gte: 151,
                  $lte: 300,
                },
              },
            },
          ],
          sillas81_150: [
            {
              $match: {
                puntoSillas: {
                  $gte: 81,
                  $lte: 151,
                },
              },
            },
          ],
          sillas41_80: [
            {
              $match: {
                puntoSillas: {
                  $gte: 41,
                  $lte: 80,
                },
              },
            },
          ],
          sillas11_40: [
            {
              $match: {
                puntoSillas: {
                  $gte: 11,
                  $lte: 40,
                },
              },
            },
          ],
          sillas0_10: [
            {
              $match: {
                puntoSillas: {
                  $gte: 1,
                  $lte: 10,
                },
              },
            },
          ],
        },
      },
      {
        $project: {
          cant_sillas: [
            {
              label: "1-10",
              count: { $sum: "$sillas0_10.costoTotalProducto" },
            },
            {
              label: "11-40",
              count: { $sum: "$sillas11_40.costoTotalProducto" },
            },
            {
              label: "41-80",
              count: { $sum: "$sillas41_80.costoTotalProducto" },
            },
            {
              label: "81-150",
              count: { $sum: "$sillas81_150.costoTotalProducto" },
            },
            {
              label: "151-300",
              count: { $sum: "$sillas151_300.costoTotalProducto" },
            },
            {
              label: "301-500",
              count: { $sum: "$sillas301_500.costoTotalProducto" },
            },
            {
              label: "+500",
              count: { $sum: "$sillas501.costoTotalProducto" },
            },
          ],
        },
      },
    ]).exec(callback);
  },
  /** Ventas  distribuidor por domicilios o no */
  getInformeDistribuidorVentasDomicilios: function (query, callback) {
    let ObjectId = require("mongoose").Types.ObjectId;
    this.aggregate([
      {
        $match: {
          idDistribuidor: new ObjectId(query.idDistribuidor),
          createdAt: {
            $gte: new Date(query.inicio),
            $lte: new Date(query.fin),
          },
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
          $or: [
            { "data_pedido.estado": "Entregado" },
            { "data_pedido.estado": "Recibido" },
            { "data_pedido.estado": "Calificado" },
          ],
        },
      },
      {
        $facet: {
          con_domicilio: [
            {
              $match: {
                puntoDomicilio: true,
              },
            },
            {
              $group: {
                _id: "$puntoDomicilio",
                total: { $sum: "$costoTotalProducto" },
              },
            },
          ],
          sin_Domicilio: [
            {
              $match: {
                puntoDomicilio: false,
              },
            },
            {
              $group: {
                _id: "$puntoDomicilio",
                total: { $sum: "$costoTotalProducto" },
              },
            },
          ],
        },
      },
    ]).exec(callback);
  },
  /** TABLA Ventas distribuidor */
  getInformeDistribuidorTablaVentas: function (query, callback) {
    let ObjectId = require("mongoose").Types.ObjectId;
    this.aggregate([
      {
        $match: {
          idDistribuidor: new ObjectId(query.idDistribuidor),
        },
      },
      {
        $match: {
          $or: [
            { estaActTraking: "Entregado" },
            { estaActTraking: "Recibido" },
            { estaActTraking: "Calificado" },
          ],
        },
      },
      {
        $lookup: {
          from: "punto_entregas",
          localField: "idPunto", //Nombre del campo de la coleccion actual
          foreignField: "_id", //Nombre del campo de la coleccion a relacionar
          pipeline: [
            {
              $project: {
                nombre: "$nombre",
                ciudad: "$ciudad",
                sillas: "$sillas",
                domicilios: "$domicilios",
                departamento: "$departamento",
                pais: "$pais",
                usuario_horeca: "$usuario_horeca",
              },
            },
          ],
          as: "data_punto", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
      {
        $lookup: {
          from: "usuario_horecas", //Nombre de la colecccion a relacionar
          localField: "data_punto.usuario_horeca", //Nombre del campo de la coleccion actual
          foreignField: "_id", //Nombre del campo de la coleccion a relacionar
          pipeline: [
            {
              $project: {
                tipo_usuario: "$tipo_usuario",
                nit: "$nit",
                nombre_establecimiento: "$nombre_establecimiento",
                tipo_negocio: "$tipo_negocio",
              },
            },
          ],
          as: "data_horeca", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
      {
        $lookup: {
          from: "pedidos",
          localField: "idPedido",
          foreignField: "_id",
          pipeline: [
            {
              $project: {
                id_pedido: "$id_pedido",
              },
            },
          ],
          as: "data_pedido",
        },
      },
      {
        $lookup: {
          from: "distribuidores_vinculados",
          let: {
            punto: "$idPunto",
            distribuidor: "$idDistribuidor",
          },
          /** Retorna solo las compras entre punto y distribuidor */
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
                from: "trabajadors", //Nombre de la colecccion a relacionar
                localField: "vendedor", //Nombre del campo de la coleccion actual
                foreignField: "_id", //Nombre del campo de la coleccion a relacionar
                pipeline: [
                  {
                    $project: {
                      nombre: { $concat: ["$nombres", " ", "$apellidos"] },
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
          from: "categoria_productos",
          localField: "categoriaProducto",
          foreignField: "_id",
          pipeline: [
            {
              $project: {
                nombre: "$nombre",
              },
            },
          ],
          as: "data_categoria",
        },
      },
      {
        $lookup: {
          from: "marca_productos",
          localField: "marcaProducto",
          foreignField: "_id",
          pipeline: [
            {
              $project: {
                nombre: "$nombre",
              },
            },
          ],
          as: "data_marca",
        },
      },
      {
        $lookup: {
          from: "organizacions", //Nombre de la colecccion a relacionar
          localField: "idOrganizacion", //Nombre del campo de la coleccion actual
          foreignField: "_id", //Nombre del campo de la coleccion a relacionar
          pipeline: [
            {
              $project: {
                nombre: "$nombre",
              },
            },
          ],
          as: "data_organizacion", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
      {
        $lookup: {
          from: "productos",
          localField: "productoId",
          foreignField: "_id",
          as: "producto_data_actual",
        },
      },
      {
        $addFields: {
          linea_producto: {
            $arrayElemAt: ["$producto_data_actual.linea_producto", 0],
          },
          producto_costo_total: {
            $multiply: ["$unidadesCompradas", "$costoProductos"],
          },
          producto_puntos_ganados: {
            $multiply: [
              "$unidadesCompradas",
              {
                $arrayElemAt: ["$detalleProducto.precios.puntos_ft_unidad", 0],
              },
            ],
          },
          producto_precios: {
            $arrayElemAt: ["$detalleProducto.precios", 0],
          },
        },
      },
      {
        $lookup: {
          from: "linea_productos",
          localField: "linea_producto",
          foreignField: "_id",
          pipeline: [
            {
              $project: {
                nombre: "$nombre",
              },
            },
          ],
          as: "data_linea",
        },
      },
      {
        $project: {
          _id: "$_id",
          pedido_fecha: "$createdAt",
          pedido_id: {
            $arrayElemAt: ["$data_pedido.id_pedido", 0],
          },
          horeca_nombre: {
            $arrayElemAt: ["$data_horeca.nombre_establecimiento", 0],
          },
          horeca_tipo_usuario: {
            $arrayElemAt: ["$data_horeca.tipo_usuario", 0],
          },
          horeca_nit: { $arrayElemAt: ["$data_horeca.nit", 0] },
          horeca_tipo_negocio: {
            $arrayElemAt: ["$data_horeca.tipo_negocio", 0],
          },
          punto_pais: { $arrayElemAt: ["$data_punto.pais", 0] },
          punto_departamento: {
            $arrayElemAt: ["$data_punto.departamento", 0],
          },
          punto_ciudad: { $arrayElemAt: ["$data_punto.ciudad", 0] },
          punto_nombre: { $arrayElemAt: ["$data_punto.nombre", 0] },
          punto_sillas: { $arrayElemAt: ["$data_punto.sillas", 0] },
          punto_domicilios: { $arrayElemAt: ["$data_punto.domicilios", 0] },
          producto_nombre: "$nombreProducto",
          producto_categoria: {
            $arrayElemAt: ["$data_categoria.nombre", 0],
          },
          producto_linea: {
            $arrayElemAt: ["$data_linea.nombre", 0],
          },
          producto_marca: {
            $arrayElemAt: ["$data_marca.nombre", 0],
          },
          producto_organizacion: {
            $arrayElemAt: ["$data_organizacion.nombre", 0],
          },
          producto_codigo_organizacion: "$codigo_organizacion_producto",
          producto_codigo_distribuidor: "$codigoDistribuidorProducto",
          producto_descripcion: "$detalleProducto.descripcion",
          producto_unidad_medida: "$producto_precios.unidad_medida",
          producto_cantidad_medida: "$producto_precios.cantidad_medida",
          pedido_cajas: "$caja",
          pedido_unidades: "$unidadesCompradas",
          pedido_total: "$producto_costo_total",
          pedido_puntos_acumulados: "$producto_puntos_ganados",
          equipo_comercial: {
            $arrayElemAt: [
              {
                $arrayElemAt: ["$data_vinculacion.data_trabajador.nombre", 0],
              },
              0,
            ],
          },
        },
      },
      {
        $sort: { pedido_fecha: 1 },
      },
    ]).exec(callback);
  },
  /** TABLA Ventas distribuidor */
  getInformeDistribuidorTablaVentas_informe: async function (query, callback) {
    const parametrizacionData = await Parametrizacion.findOne();
    const valor_punto_feat = parametrizacionData.valor_1puntoft;

    let ObjectId = require("mongoose").Types.ObjectId;
    let busqueda
    console.log('idDistribuidor', query.idDistribuidor)
    if (query.tipo === 'curso') {
      busqueda = [
        { estaActTraking: "Aprobado Externo" },
        { estaActTraking: "Alistamiento" },
        { estaActTraking: "Despachado" },
        { estaActTraking: "Facturado" },
        { estaActTraking: "Pendiente" },
      ]
    } else {
      busqueda = [
        { estaActTraking: "Entregado" },
        { estaActTraking: "Recibido" },
        { estaActTraking: "Calificado" },
      ]
    }
    this.aggregate([
      {
        $match: {
          idDistribuidor: new ObjectId(query.idDistribuidor),
        },
      },
      {
        $match: {
          $or: busqueda,
        },
      },
      {
        $lookup: {
          from: "punto_entregas",
          localField: "idPunto", //Nombre del campo de la coleccion actual
          foreignField: "_id", //Nombre del campo de la coleccion a relacionar
          pipeline: [
            {
              $project: {
                nombre: "$nombre",
                ciudad: "$ciudad",
                sillas: "$sillas",
                domicilios: "$domicilios",
                departamento: "$departamento",
                pais: "$pais",
                usuario_horeca: "$usuario_horeca",
                direccion: "$direccion",
                _id: "$_id",

              },
            },
          ],
          as: "data_punto", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },

      {
        $lookup: {
          from: "usuario_horecas", //Nombre de la colecccion a relacionar
          localField: "data_punto.usuario_horeca", //Nombre del campo de la coleccion actual
          foreignField: "_id", //Nombre del campo de la coleccion a relacionar
          pipeline: [
            {
              $project: {
                tipo_usuario: "$tipo_usuario",
                nit: "$nit",
                nombre_establecimiento: "$nombre_establecimiento",
                razon_social: "$razon_social",
                tipo_negocio: "$tipo_negocio",
              },
            },
          ],
          as: "data_horeca", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
      {
        $lookup: {
          from: "pedidos",
          localField: "idPedido",
          foreignField: "_id",
          pipeline: [

            {
              $project: {
                id_pedido: "$id_pedido",
                codigo_descuento: "$codigo_descuento"
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

          ],
          as: "data_pedido",
        },
      },
      {
        $lookup: {
          from: "distribuidores_vinculados",
          let: {
            punto: "$idPunto",
            distribuidor: "$idDistribuidor",
          },
          /** Retorna solo las compras entre punto y distribuidor */
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
                from: "trabajadors", //Nombre de la colecccion a relacionar
                localField: "vendedor", //Nombre del campo de la coleccion actual
                foreignField: "_id", //Nombre del campo de la coleccion a relacionar
                pipeline: [
                  {
                    $project: {
                      nombre: { $concat: ["$nombres", " ", "$apellidos"] },
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
          from: "categoria_productos",
          localField: "categoriaProducto",
          foreignField: "_id",
          pipeline: [
            {
              $project: {
                nombre: "$nombre",
              },
            },
          ],
          as: "data_categoria",
        },
      },
      {
        $lookup: {
          from: "marca_productos",
          localField: "marcaProducto",
          foreignField: "_id",
          pipeline: [
            {
              $project: {
                nombre: "$nombre",
              },
            },
          ],
          as: "data_marca",
        },
      },
      {
        $lookup: {
          from: "organizacions", //Nombre de la colecccion a relacionar
          localField: "idOrganizacion", //Nombre del campo de la coleccion actual
          foreignField: "_id", //Nombre del campo de la coleccion a relacionar
          pipeline: [
            {
              $project: {
                nombre: "$nombre",
              },
            },
          ],
          as: "data_organizacion", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
      {
        $lookup: {
          from: "productos",
          localField: "productoId",
          foreignField: "_id",
          as: "producto_data_actual",
        },
      },
      {
        $addFields: {
          linea_producto: {
            $arrayElemAt: ["$producto_data_actual.linea_producto", 0],
          },
          producto_costo_total: {
            $multiply: ["$unidadesCompradas", "$costoProductos"],
          },
          producto_puntos_ganados: {
            $multiply: [
              "$unidadesCompradas",
              {
                $arrayElemAt: ["$detalleProducto.precios.puntos_ft_unidad", 0],
              },
            ],
          },
          producto_precios: {
            $arrayElemAt: ["$detalleProducto.precios", 0],
          },
        },
      },
      {
        $lookup: {
          from: "linea_productos",
          localField: "linea_producto",
          foreignField: "_id",
          pipeline: [
            {
              $project: {
                nombre: "$nombre",
              },
            },
          ],
          as: "data_linea",
        },
      },
      {
        $project: {
          _id: "$_id",
          precioEspecial: "$precioEspecial",
          detalleProducto: "$detalleProducto",
          punto_direccion: { $arrayElemAt: ["$data_punto.direccion", 0] },
          pedido_fecha: "$createdAt",
          estadoPedido: "$estaActTraking",
          costoProductos: "$costoProductos",
          unidadesCompradas: "$unidadesCompradas",

          producto_unidades_caja: ["$producto_data_actual.precios.und_x_caja", 0],
          precioActual: {
            $arrayElemAt: ["$producto_data_actual.precios.precio_unidad", 0],
          },
          preciosprecios: {
            $arrayElemAt: ["$producto_data_actual.precios", 0],
          },
          precioDescuento: {
            $arrayElemAt: ["$producto_data_actual.precios.precio_descuento", 0],
          },
          porcentajeDescuento: {
            $arrayElemAt: ["$producto_data_actual.prodPorcentajeDesc", 0],
          },
          valor_punto_feat: valor_punto_feat,
          data_pedido: {
            $arrayElemAt: ["$data_pedido", 0],
          },
          pedido_id: {
            $arrayElemAt: ["$data_pedido.id_pedido", 0],
          },
          horeca_nombre: {
            $arrayElemAt: ["$data_horeca.nombre_establecimiento", 0],
          },
          horeca_razon_social: {
            $arrayElemAt: ["$data_horeca.razon_social", 0],
          },
          horeca_tipo_usuario: {
            $arrayElemAt: ["$data_horeca.tipo_usuario", 0],
          },
          horeca_nit: { $arrayElemAt: ["$data_horeca.nit", 0] },
          horeca_tipo_negocio: {
            $arrayElemAt: ["$data_horeca.tipo_negocio", 0],
          },
          punto_pais: { $arrayElemAt: ["$data_punto.pais", 0] },
          punto_departamento: {
            $arrayElemAt: ["$data_punto.departamento", 0],
          },
          idPunto: {
            $arrayElemAt: ["$data_punto._id", 0],
          },
          punto_ciudad: { $arrayElemAt: ["$data_punto.ciudad", 0] },
          punto_nombre: { $arrayElemAt: ["$data_punto.nombre", 0] },
          punto_sillas: { $arrayElemAt: ["$data_punto.sillas", 0] },
          punto_domicilios: { $arrayElemAt: ["$data_punto.domicilios", 0] },
          producto_nombre: "$nombreProducto",
          producto_categoria: {
            $arrayElemAt: ["$data_categoria.nombre", 0],
          },
          producto_linea: {
            $arrayElemAt: ["$data_linea.nombre", 0],
          },
          producto_marca: {
            $arrayElemAt: ["$data_marca.nombre", 0],
          },
          producto_organizacion: {
            $arrayElemAt: ["$data_organizacion.nombre", 0],
          },
          producto_codigo_organizacion: "$codigo_organizacion_producto",
          producto_codigo_distribuidor: "$codigoDistribuidorProducto",
          producto_descripcion: "$detalleProducto.descripcion",
          producto_unidad_medida: "$producto_precios.unidad_medida",
          producto_cantidad_medida: "$producto_precios.cantidad_medida",
          pedido_cajas: "$caja",
          pedido_unidades: "$unidadesCompradas",
          pedido_total: "$producto_costo_total",
          pedido_puntos_acumulados: "$producto_puntos_ganados",
          equipo_comercial: {
            $arrayElemAt: [
              {
                $arrayElemAt: ["$data_vinculacion.data_trabajador.nombre", 0],
              },
              0,
            ],
          },
        },
      },
      {
        $sort: { pedido_fecha: 1 },
      },
    ]).exec(callback);
  },
  /***************** Sector DISTRIBUIDOR ****************/
  /** PARTICIPACION VENTAS EN EL MERCADO */
  informeDistribuidorSectorVentas: function (query, ciudad, callback) {
    let ObjectId = require("mongoose").Types.ObjectId;
    this.aggregate([
      // Se agrega campo para que cuando venga null(sin filtro), se muestre toda la data
      {
        $addFields: {
          ciudad_query: ciudad.ciudad,
        },
      },
      {
        $match: {
          $or: [{ ciudad: ciudad.ciudad }, { ciudad_query: null }],
        },
      },
      /** Arma la data de la grafica */
      {
        $addFields: {
          costoTotalProducto: {
            $multiply: ["$unidadesCompradas", "$costoProductos"],
          },
        },
      },
      {
        $facet: {
          mi_distribuidor: [
            {
              $match: {
                idDistribuidor: new ObjectId(query.idDistribuidor),
              },
            },
          ],
          otros_distribuidores: [
            {
              $match: {
                idDistribuidor: {
                  $ne: new ObjectId(query.idDistribuidor),
                },
              },
            },
          ],
        },
      },
      {
        $project: {
          mi_distribuidor: [
            {
              label: "Distribuidor",
              total: { $sum: "$mi_distribuidor.costoTotalProducto" },
            },
          ],
          otros_distribuidores: [
            {
              label: "Otros distribuidores",
              total: { $sum: "$otros_distribuidores.costoTotalProducto" },
            },
          ],
        },
      },
    ]).exec(callback);
  },
  /** NUMERO DE CLIENTES EN EL MERCADO */
  informeDistribuidorSectorClientes: function (query, ciudad, callback) {
    let ObjectId = require("mongoose").Types.ObjectId;
    this.aggregate([
      // Se agrega campo para que cuando venga null(sin filtro), se muestre toda la data
      {
        $addFields: {
          ciudad_query: ciudad.ciudad,
        },
      },
      {
        $match: {
          $or: [{ ciudad: ciudad.ciudad }, { ciudad_query: null }],
        },
      },
      {
        $group: {
          _id: {
            idDistribuidor: "$idDistribuidor",
            idPunto: "$idPunto",
          },
          total: { $sum: 1 },
          idPunto: { $first: "$idPunto" },
          idDistribuidor: { $first: "$idDistribuidor" },
          punto_ciudad: { $first: "$punto_ciudad" },
        },
      },
      /** Arma la data de la grafica */
      {
        $facet: {
          mi_distribuidor: [
            {
              $match: {
                idDistribuidor: new ObjectId(query.idDistribuidor),
              },
            },
          ],
          otros_distribuidores: [
            {
              $match: {
                idDistribuidor: {
                  $ne: new ObjectId(query.idDistribuidor),
                },
              },
            },
          ],
        },
      },
      {
        $project: {
          mi_distribuidor: [
            {
              label: "Distribuidor",
              total: { $size: "$mi_distribuidor" },
            },
          ],
          otros_distribuidores: [
            {
              label: "Otros distribuidores",
              total: { $size: "$otros_distribuidores" },
            },
          ],
        },
      },
    ]).exec(callback);
  },
  /** NUMERO DE SILLAS EN EL MERCADO */

  informeDistribuidorSectorSillas: function (query, ciudad, callback) {
    let ObjectId = require("mongoose").Types.ObjectId;
    this.aggregate([
      // Se agrega campo para que cuando venga null(sin filtro), se muestre toda la data
      {
        $addFields: {
          ciudad_query: ciudad.ciudad,
        },
      },
      {
        $match: {
          $or: [{ ciudad: ciudad.ciudad }, { ciudad_query: null }],
        },
      },
      {
        $group: {
          _id: {
            idDistribuidor: "$idDistribuidor",
            idPunto: "$idPunto",
          },
          total: { $sum: "$puntoSillas" },
          idPunto: { $first: "$idPunto" },
          puntoSillas: { $first: "$puntoSillas" },
          idDistribuidor: { $first: "$idDistribuidor" },
        },
      },
      {
        $facet: {
          mi_distribuidor: [
            {
              $match: {
                idDistribuidor: new ObjectId(query.idDistribuidor),
              },
            },
          ],
          otros_distribuidores: [
            {
              $match: {
                idDistribuidor: {
                  $ne: new ObjectId(query.idDistribuidor),
                },
              },
            },
          ],
        },
      },
      {
        $project: {
          mi_distribuidor: [
            {
              label: "Distribuidor",
              total: { $sum: "$mi_distribuidor.puntoSillas" },
            },
          ],
          otros_distribuidores: [
            {
              label: "Otros distribuidores",
              total: { $sum: "$otros_distribuidores.puntoSillas" },
            },
          ],
        },
      },
    ]).exec(callback);
  },
  /** PARTICIPACION EN CATEGORIAS DEL MERCADO */
  informeDistribuidorSectorCategorias: function (query, callback) {
    let ObjectId = require("mongoose").Types.ObjectId;
    if (query.idCat == "todos") {
      this.aggregate([
        {
          $addFields: {
            costoTotalProducto: {
              $multiply: ["$unidadesCompradas", "$costoProductos"],
            },
          },
        },
        {
          $facet: {
            mi_distribuidor: [
              {
                $match: {
                  idDistribuidor: new ObjectId(query.idDistribuidor),
                },
              },
            ],
            otros_distribuidores: [
              {
                $match: {
                  idDistribuidor: {
                    $ne: new ObjectId(query.idDistribuidor),
                  },
                },
              },
            ],
          },
        },
        {
          $project: {
            mi_distribuidor: [
              {
                label: "Distribuidor",
                total: { $sum: "$mi_distribuidor.costoTotalProducto" },
              },
            ],
            otros_distribuidores: [
              {
                label: "Otros distribuidores",
                total: { $sum: "$otros_distribuidores.costoTotalProducto" },
              },
            ],
          },
        },
      ]).exec(callback);
    } else {
      this.aggregate([
        {
          $match: {
            categoriaProducto: new ObjectId(query.idCat),
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
          $facet: {
            mi_distribuidor: [
              {
                $match: {
                  idDistribuidor: new ObjectId(query.idDistribuidor),
                },
              },
            ],
            otros_distribuidores: [
              {
                $match: {
                  idDistribuidor: {
                    $ne: new ObjectId(query.idDistribuidor),
                  },
                },
              },
            ],
          },
        },
        {
          $project: {
            mi_distribuidor: [
              {
                label: "Distribuidor",
                total: { $sum: "$mi_distribuidor.costoTotalProducto" },
              },
            ],
            otros_distribuidores: [
              {
                label: "Otros distribuidores",
                total: { $sum: "$otros_distribuidores.costoTotalProducto" },
              },
            ],
          },
        },
      ]).exec(callback);
    }
  },
  informeDistribuidorSectorLinea: function (query, callback) {
    let ObjectId = require("mongoose").Types.ObjectId;
    if (query.idLinea == "todos") {
      this.aggregate([
        {
          $addFields: {
            costoTotalProducto: {
              $multiply: ["$unidadesCompradas", "$costoProductos"],
            },
          },
        },
        {
          $facet: {
            mi_distribuidor: [
              {
                $match: {
                  idDistribuidor: new ObjectId(query.idDistribuidor),
                },
              },
            ],
            otros_distribuidores: [
              {
                $match: {
                  idDistribuidor: {
                    $ne: new ObjectId(query.idDistribuidor),
                  },
                },
              },
            ],
          },
        },
        {
          $project: {
            mi_distribuidor: [
              {
                label: "Distribuidor",
                total: { $sum: "$mi_distribuidor.costoTotalProducto" },
              },
            ],
            otros_distribuidores: [
              {
                label: "Otros distribuidores",
                total: { $sum: "$otros_distribuidores.costoTotalProducto" },
              },
            ],
          },
        },
      ]).exec(callback);
    } else {
      this.aggregate([
        {
          $match: {
            linea_producto: new ObjectId(query.idLinea),
          },
        },
        /** Arma la data de la grafica */
        {
          $addFields: {
            costoTotalProducto: {
              $multiply: ["$unidadesCompradas", "$costoProductos"],
            },
          },
        },
        {
          $facet: {
            mi_distribuidor: [
              {
                $match: {
                  idDistribuidor: new ObjectId(query.idDistribuidor),
                },
              },
            ],
            otros_distribuidores: [
              {
                $match: {
                  idDistribuidor: {
                    $ne: new ObjectId(query.idDistribuidor),
                  },
                },
              },
            ],
          },
        },
        {
          $project: {
            mi_distribuidor: [
              {
                label: "Distribuidor",
                total: { $sum: "$mi_distribuidor.costoTotalProducto" },
              },
            ],
            otros_distribuidores: [
              {
                label: "Otros distribuidores",
                total: { $sum: "$otros_distribuidores.costoTotalProducto" },
              },
            ],
          },
        },
      ]).exec(callback);
    }
  },
  informeDistribuidorSectortipoNegocio: function (
    query,
    tipo_negocio,
    callback
  ) {
    let ObjectId = require("mongoose").Types.ObjectId;
    this.aggregate([
      {
        $match: {
          tipoUsuario: tipo_negocio.tipo_negocio,
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
        $project: {
          tipo_negocio: "$tipoUsuario",
          costoTotalProducto: "$costoTotalProducto",
          idDistribuidor: "$idDistribuidor",
        },
      },
      {
        $facet: {
          mi_distribuidor: [
            {
              $match: {
                idDistribuidor: new ObjectId(query.idDistribuidor),
              },
            },
          ],
          otros_distribuidores: [
            {
              $match: {
                idDistribuidor: {
                  $ne: new ObjectId(query.idDistribuidor),
                },
              },
            },
          ],
        },
      },
      {
        $project: {
          mi_distribuidor: [
            {
              label: "Distribuidor",
              total: { $sum: "$mi_distribuidor.costoTotalProducto" },
            },
          ],
          otros_distribuidores: [
            {
              label: "Otros distribuidores",
              total: { $sum: "$otros_distribuidores.costoTotalProducto" },
            },
          ],
        },
      },
    ]).exec(callback);
  },
  getGraficasMarcasMeses: function (query, callback) {
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
          from: "marca_productos", //Nombre de la colecccion a relacionar
          localField: "marcaProducto", //Nombre del campo de la coleccion actual
          foreignField: "_id", //Nombre del campo de la coleccion a relacionar
          as: "marca_productos_query", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
      {
        $project: {
          label: "$marca_productos_query.nombre",
          total: { $sum: "$totalCompra" },
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
  },
  ventasAcumCat: function (query, callback) {
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
        $match: {
          $or: [
            { estaActTraking: "Entregado" },
            { estaActTraking: "Recibido" },
            { estaActTraking: "Calificado" },
          ],
        },
      },
      {
        $lookup: {
          from: "categoria_productos", //Nombre de la colecccion a relacionar
          localField: "categoriaProducto", //Nombre del campo de la coleccion actual
          foreignField: "_id", //Nombre del campo de la coleccion a relacionar
          as: "categoria_productos_query", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
      {
        $project: {
          label: "$categoria_productos_query.nombre",
          total: { $multiply: ["$costoProductos", "$unidadesCompradas"] },
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
  },
  ventasAcumMarcas: function (query, callback) {
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
        $match: {
          $or: [
            { estaActTraking: "Entregado" },
            { estaActTraking: "Recibido" },
            { estaActTraking: "Calificado" },
          ],
        },
      },
      {
        $lookup: {
          from: "marca_productos", //Nombre de la colecccion a relacionar
          localField: "marcaProducto", //Nombre del campo de la coleccion actual
          foreignField: "_id", //Nombre del campo de la coleccion a relacionar
          as: "marca_productos_query", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
      {
        $project: {
          label: "$marca_productos_query.nombre",
          total: { $multiply: ["$costoProductos", "$unidadesCompradas"] },
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
  },
  ventasAcumOrg: function (query, callback) {
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
        $match: {
          $or: [
            { estaActTraking: "Entregado" },
            { estaActTraking: "Recibido" },
            { estaActTraking: "Calificado" },
          ],
        },
      },
      {
        $lookup: {
          from: "organizacions", //Nombre de la colecccion a relacionar
          localField: "idOrganizacion", //Nombre del campo de la coleccion actual
          foreignField: "_id", //Nombre del campo de la coleccion a relacionar
          as: "dataOrg", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
      {
        $project: {
          label: "$dataOrg.nombre",
          total: { $multiply: ["$costoProductos", "$unidadesCompradas"] },
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
  },
  ventasAcumDist: function (query, callback) {
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
        $match: {
          $or: [
            { estaActTraking: "Entregado" },
            { estaActTraking: "Recibido" },
            { estaActTraking: "Calificado" },
          ],
        },
      },
      {
        $lookup: {
          from: "distribuidors", //Nombre de la colecccion a relacionar
          localField: "idDistribuidor", //Nombre del campo de la coleccion actual
          foreignField: "_id", //Nombre del campo de la coleccion a relacionar
          as: "dataDistribuidor", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
      {
        $project: {
          label: "$dataDistribuidor.nombre",
          total: { $multiply: ["$costoProductos", "$unidadesCompradas"] },
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
  },
  ventasAcumCiudades: function (query, callback) {
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
        $match: {
          $or: [
            { estaActTraking: "Entregado" },
            { estaActTraking: "Recibido" },
            { estaActTraking: "Calificado" },
          ],
        },
      },
      {
        $project: {
          label: "$ciudad",
          total: { $multiply: ["$costoProductos", "$unidadesCompradas"] },
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
  },
  ventasAcumTipoNegocio: function (query, callback) {
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
        $match: {
          $or: [
            { estaActTraking: "Entregado" },
            { estaActTraking: "Recibido" },
            { estaActTraking: "Calificado" },
          ],
        },
      },
      {
        $project: {
          label: "$tipoUsuario",
          total: { $multiply: ["$costoProductos", "$unidadesCompradas"] },
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
  },
  establecimientosAcumCat: function (query, callback) {
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
          localField: "categoriaProducto", //Nombre del campo de la coleccion actual
          foreignField: "_id", //Nombre del campo de la coleccion a relacionar
          as: "categoria_productos_query", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
      {
        $lookup: {
          from: "usuario_horecas", //Nombre de la colecccion a relacionar
          localField: "idComprador", //Nombre del campo de la coleccion actual
          foreignField: "_id", //Nombre del campo de la coleccion a relacionar
          as: "comprador_productos_query", //Nombre del campo donde se insertara todos los documentos relacionados
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
  },
  sillasCatGraficas: function (query, callback) {
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
          localField: "categoriaProducto", //Nombre del campo de la coleccion actual
          foreignField: "_id", //Nombre del campo de la coleccion a relacionar
          as: "categoria_productos_query", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
      {
        $lookup: {
          from: "usuario_horecas", //Nombre de la colecccion a relacionar
          localField: "idComprador", //Nombre del campo de la coleccion actual
          foreignField: "_id", //Nombre del campo de la coleccion a relacionar
          as: "comprador_productos_query", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
      {
        $project: {
          label: "$categoria_productos_query.nombre",
          total: { $sum: "$puntoSillas" },
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
  },
  getListaVentas: function (query, callback) {
    this.aggregate([
      {
        $match: {
          $or: [
            { estaActTraking: "Entregado" },
            { estaActTraking: "Recibido" },
            { estaActTraking: "Calificado" },
          ],
        },
      },
      {
        $lookup: {
          from: "distribuidors", //Nombre de la colecccion a relacionar
          localField: "idDistribuidor", //Nombre del campo de la coleccion actual
          foreignField: "_id", //Nombre del campo de la coleccion a relacionar
          as: "dataDistribuidor", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
      {
        $lookup: {
          from: "organizacions", //Nombre de la colecccion a relacionar
          localField: "idOrganizacion", //Nombre del campo de la coleccion actual
          foreignField: "_id", //Nombre del campo de la coleccion a relacionar
          as: "dataOrganizacion", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
      {
        $lookup: {
          from: "categoria_productos", //Nombre de la colecccion a relacionar
          localField: "categoriaProducto", //Nombre del campo de la coleccion actual
          foreignField: "_id", //Nombre del campo de la coleccion a relacionar
          as: "dataCategoria", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
      {
        $lookup: {
          from: "linea_productos", //Nombre de la colecccion a relacionar
          localField: "lineaProducto", //Nombre del campo de la coleccion actual
          foreignField: "_id", //Nombre del campo de la coleccion a relacionar
          as: "dataLinea", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
      {
        $lookup: {
          from: "marca_productos", //Nombre de la colecccion a relacionar
          localField: "marcaProducto", //Nombre del campo de la coleccion actual
          foreignField: "_id", //Nombre del campo de la coleccion a relacionar
          as: "dataMarca", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
      {
        $lookup: {
          from: "usuario_horecas", //Nombre de la colecccion a relacionar
          localField: "idUserHoreca", //Nombre del campo de la coleccion actual
          foreignField: "_id", //Nombre del campo de la coleccion a relacionar
          as: "dataComprador", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
      {
        $lookup: {
          from: "punto_entregas", //Nombre de la colecccion a relacionar
          localField: "idPunto", //Nombre del campo de la coleccion actual
          foreignField: "_id", //Nombre del campo de la coleccion a relacionar
          as: "dataPunto", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
      {
        $lookup: {
          from: "pedidos", //Nombre de la colecccion a relacionar
          localField: "idPedido", //Nombre del campo de la coleccion actual
          foreignField: "_id", //Nombre del campo de la coleccion a relacionar
          as: "dataPedido", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
      {
        $sort: {
          createdAt: -1,
          cantidad: -1,
        },
      },

      {
        $project: {
          nombreDistribuidor: "$dataDistribuidor.nombre",
          nitDistribuidor: "$dataDistribuidor.nit_cc",
          tipoDistribuidor: "$dataDistribuidor.tipo",
          fechaPedido: {
            $dateToString: {
              format: "%Y-%m-%d",
              date: "$createdAt",
            },
          },
          numeroPedido: "$dataPedido.id_pedido",
          nombreEstablecimiento: "$dataComprador.nombre_establecimiento",
          nitEstablecimiento: "$dataComprador.nit",
          tipoEstablecimiento: "$dataComprador.tipo_negocio",
          dptoPunto: "$dataPunto.departamento",
          ciudadPunto: "$dataPunto.ciudad",
          puntoEntrega: "$dataPunto.nombre",
          categoria: "$dataCategoria.nombre",
          linea: "$dataLinea.nombre",
          organizacion: "$dataOrganizacion.nombre",
          marca: "$dataMarca.nombre",
          codigoFT: "$codigoFeatProducto",
          codigoOrg: "$codigo_organizacion_producto",
          codigoDistri: "$codigoDistribuidorProducto",
          descripcion_producto: "$detalleProducto.nombre",
          tam_cantidad: "",
          cajas_solicitadas: "",
          unidades_solicitadas: "$unidadesCompradas",
          um: "$detalleProducto.precios.unidad_medida",
          valor_total: "$costoProductos",
          puntos_acumulados: "$puntosGanados",
          vendedor: "",
          //total: { $sum: '$puntoSillas' },
        },
      },
    ]).exec(callback);
  },
  getListaPedidos: function (query, callback) {
    this.aggregate([
      {
        $match: {},
      },
      {
        $lookup: {
          from: "distribuidors", //Nombre de la colecccion a relacionar
          localField: "idDistribuidor", //Nombre del campo de la coleccion actual
          foreignField: "_id", //Nombre del campo de la coleccion a relacionar
          as: "dataDistribuidor", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
      {
        $lookup: {
          from: "organizacions", //Nombre de la colecccion a relacionar
          localField: "idOrganizacion", //Nombre del campo de la coleccion actual
          foreignField: "_id", //Nombre del campo de la coleccion a relacionar
          as: "dataOrganizacion", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
      {
        $lookup: {
          from: "categoria_productos", //Nombre de la colecccion a relacionar
          localField: "categoriaProducto", //Nombre del campo de la coleccion actual
          foreignField: "_id", //Nombre del campo de la coleccion a relacionar
          as: "dataCategoria", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
      {
        $lookup: {
          from: "linea_productos", //Nombre de la colecccion a relacionar
          localField: "lineaProducto", //Nombre del campo de la coleccion actual
          foreignField: "_id", //Nombre del campo de la coleccion a relacionar
          as: "dataLinea", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
      {
        $lookup: {
          from: "marca_productos", //Nombre de la colecccion a relacionar
          localField: "marcaProducto", //Nombre del campo de la coleccion actual
          foreignField: "_id", //Nombre del campo de la coleccion a relacionar
          as: "dataMarca", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
      {
        $lookup: {
          from: "usuario_horecas", //Nombre de la colecccion a relacionar
          localField: "idUserHoreca", //Nombre del campo de la coleccion actual
          foreignField: "_id", //Nombre del campo de la coleccion a relacionar
          as: "dataComprador", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
      {
        $lookup: {
          from: "punto_entregas", //Nombre de la colecccion a relacionar
          localField: "idPunto", //Nombre del campo de la coleccion actual
          foreignField: "_id", //Nombre del campo de la coleccion a relacionar
          as: "dataPunto", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
      {
        $lookup: {
          from: "pedidos", //Nombre de la colecccion a relacionar
          localField: "idPedido", //Nombre del campo de la coleccion actual
          foreignField: "_id", //Nombre del campo de la coleccion a relacionar
          as: "dataPedido", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
      {
        $sort: { cantidad: -1 },
      },
      {
        $project: {
          nombreDistribuidor: "$dataDistribuidor.nombre",
          nitDistribuidor: "$dataDistribuidor.nit_cc",
          tipoDistribuidor: "$dataDistribuidor.tipo",
          fechaPedido: "$createdAt",
          numeroPedido: "$dataPedido.id_pedido",
          nombreEstablecimiento: "$dataComprador.nombre_establecimiento",
          nitEstablecimiento: "$dataComprador.nit",
          tipoEstablecimiento: "$dataComprador.tipo_negocio",
          dataComprador: "$dataComprador",
          dptoPunto: "$dataPunto.departamento",
          ciudadPunto: "$dataPunto.ciudad",
          puntoEntrega: "$dataPunto.nombre",
          categoria: "$dataCategoria.nombre",
          linea: "$dataLinea.nombre",
          organizacion: "$dataOrganizacion.nombre",
          marca: "$dataMarca.nombre",
          codigoFT: "$codigoFeatProducto",
          codigoOrg: "$codigo_organizacion_producto",
          codigoDistri: "$codigoDistribuidorProducto",
          descripcion_producto: "$detalleProducto.descripcion",
          tam_cantidad: "",
          cajas_solicitadas: "",
          unidades_solicitadas: "$unidadesCompradas",
          um: "$detalleProducto.precios.unidad_medida",
          valor_total: "$costoProductos",
          puntos_acumulados: "$puntosGanados",
          vendedor: "",
        },
      },
      {
        $group: {
          _id: { $first: "$numeroPedido" },
          nombreDistribuidor: { $first: "$nombreDistribuidor" },
          nitDistribuidor: { $first: "$nitDistribuidor" },
          tipoDistribuidor: { $first: "$tipoDistribuidor" },
          fechaPedido: { $first: "$fechaPedido" },
          numeroPedido: { $first: "$numeroPedido" },
          dataComprador: "$dataComprador",
          nombreEstablecimiento: { $first: "$nombreEstablecimiento" },
          nitEstablecimiento: { $first: "$nitEstablecimiento" },
          tipoEstablecimiento: { $first: "$tipoEstablecimiento" },
          dptoPunto: { $first: "$dptoPunto" },
          ciudadPunto: { $first: "$ciudadPunto" },
          puntoEntrega: { $first: "$puntoEntrega" },
          categoria: { $first: "$categoria" },
          linea: { $first: "$linea" },
          organizacion: { $first: "$organizacion" },
          marca: { $first: "$marca" },
          codigoFT: { $first: "$codigoFT" },
          codigoOrg: { $first: "$codigoOrg" },
          codigoDistri: { $first: "$codigoDistri" },
          descripcion_producto: { $first: "$descripcion_producto" },
          tam_cantidad: { $first: "$tam_cantidad" },
          cajas_solicitadas: { $first: "$cajas_solicitadas" },
          unidades_solicitadas: { $first: "$unidades_solicitadas" },
          um: { $first: "$um" },
          valor_total: { $first: "$valor_total" },
          puntos_acumulados: { $first: "$puntos_acumulados" },
          vendedor: { $first: "$vendedor" },
        },
      },
    ]).exec(callback);
  },
  /** Trae toda la informacion del componente de puntos-feat de una organizacion */
  getDataComponentePuntosFeat: function (query, callback) {
    var ObjectId = require("mongoose").Types.ObjectId;
    this.aggregate([
      {
        $match: { idOrganizacion: new ObjectId(query.idOrg) },
      },
      // Filtra por la fecha de pedido y su estado
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
            $gte: new Date(query.inicio),
            $lte: new Date(query.fin),
          },
          $or: [
            { "data_pedido.estado": "Entregado" },
            { "data_pedido.estado": "Recibido" },
            { "data_pedido.estado": "Calificado" },
          ],
        },
      },
      {
        $unwind: "$detalleProducto.precios",
      },
      // Pre-organiza la data de la consulta
      {
        $project: {
          puntos_ganados: {
            $multiply: [
              "$unidadesCompradas",
              "$detalleProducto.precios.puntos_ft_unidad",
            ],
          },
          producto_id: "$productoId",
          pedido_fecha: "$data_pedido.fecha",
          puntos_fecha_apertura: "$detalleProducto.fecha_apertura_puntosft",
          puntos_fecha_cierre: "$detalleProducto.fecha_cierre_puntosft",
        },
      },
      {
        $addFields: {
          puntos_fecha_cierre: {
            $dateFromString: {
              dateString: "$puntos_fecha_cierre",
            },
          },
          puntos_fecha_apertura: {
            $dateFromString: {
              dateString: "$puntos_fecha_apertura",
            },
          },
        },
      },
      // Filtro de fecha pedidos vs fecha puntos feat
      {
        $match: {
          $expr: {
            $and: [
              {
                $lte: ["$pedido_fecha", "$puntos_fecha_cierre"],
              },
              {
                $gte: ["$pedido_fecha", "$puntos_fecha_apertura"],
              },
            ],
          },
          puntos_ganados: {
            $gt: 0,
          },
        },
      },
      // Se busca la data actualizada del producto para mostrar en la tabla
      {
        $lookup: {
          from: "productos",
          localField: "producto_id",
          foreignField: "_id",
          pipeline: [
            {
              $project: {
                nombre: "$nombre",
                marca_producto: "$marca_producto",
                categoria_producto: "$categoria_producto",
                producto_puntosFt: {
                  $arrayElemAt: ["$precios.puntos_ft_unidad", 0],
                },
                fecha_apertura_puntosft: "$fecha_apertura_puntosft",
                fecha_cierre_puntosft: "$fecha_cierre_puntosft",
              },
            },
          ],
          as: "data_producto",
        },
      },
      {
        $addFields: {
          marca_producto: {
            $arrayElemAt: ["$data_producto.marca_producto", 0],
          },
          categoria_producto: {
            $arrayElemAt: ["$data_producto.categoria_producto", 0],
          },
        },
      },
      // Nombre marca producto
      {
        $lookup: {
          from: "marca_productos", //Nombre de la colecccion a relacionar
          localField: "marca_producto", //Nombre del campo de la coleccion actual
          foreignField: "_id", //Nombre del campo de la coleccion a relacionar
          pipeline: [
            {
              $project: {
                nombre: "$nombre",
              },
            },
          ],
          as: "marca_producto", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
      // Nombre categoria producto
      {
        $lookup: {
          from: "categoria_productos", //Nombre de la colecccion a relacionar
          localField: "categoria_producto", //Nombre del campo de la coleccion actual
          foreignField: "_id", //Nombre del campo de la coleccion a relacionar
          pipeline: [
            {
              $project: {
                nombre: "$nombre",
              },
            },
          ],
          as: "categoria_producto", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
      // Se agrupa por ID producto
      {
        $group: {
          _id: "$producto_id",
          puntos_ganados: { $sum: "$puntos_ganados" },
          producto_id: { $first: "$producto_id" },
          data_producto: { $first: "$data_producto" },
          producto_marca: { $first: "$producto_marca" },
          producto_categoria: { $first: "$producto_categoria" },
          producto_marca: {
            $first: { $arrayElemAt: ["$marca_producto.nombre", 0] },
          },
          producto_categoria: {
            $first: { $arrayElemAt: ["$categoria_producto.nombre", 0] },
          },
        },
      },
      {
        $project: {
          _id: "$producto_id",
          puntosFt_ganados: "$puntos_ganados",
          producto: {
            $arrayElemAt: ["$data_producto.nombre", 0],
          },
          puntosFt_asignados: {
            $arrayElemAt: ["$data_producto.producto_puntosFt", 0],
          },
          puntosFt_fecha_inicio: {
            $arrayElemAt: ["$data_producto.fecha_apertura_puntosft", 0],
          },
          puntosFt_fecha_cierre: {
            $arrayElemAt: ["$data_producto.fecha_cierre_puntosft", 0],
          },
          producto_marca: "$producto_marca",
          producto_categoria: "$producto_categoria",
        },
      },
      {
        $facet: {
          producto: [
            {
              $addFields: {
                label_to_sort: {
                  $toLower: "$producto",
                },
              },
            },
            {
              $sort: { label_to_sort: 1 },
            },
          ],
          marca: [
            {
              $group: {
                _id: "$producto_marca",
                marca: { $first: "$producto_marca" },
                puntosFt_asignados: { $first: "$puntosFt_asignados" },
                puntosFt_ganados: { $sum: "$puntosFt_ganados" },
              },
            },
            {
              $addFields: {
                label_to_sort: {
                  $toLower: "$marca",
                },
              },
            },
            {
              $sort: { label_to_sort: 1 },
            },
          ],
          categoria: [
            {
              $group: {
                _id: "$producto_categoria",
                categoria: { $first: "$producto_categoria" },
                puntosFt_asignados: { $first: "$puntosFt_asignados" },
                puntosFt_ganados: { $sum: "$puntosFt_ganados" },
              },
            },
            {
              $addFields: {
                label_to_sort: {
                  $toLower: "$categoria",
                },
              },
            },
            {
              $sort: { label_to_sort: 1 },
            },
          ],
        },
      },
    ]).exec(callback);
  },
  informesClientesPorMes: function (query, callback) {
    var ObjectId = require("mongoose").Types.ObjectId;
    this.aggregate([
      {
        $match: {
          idDistribuidor: new ObjectId(query.idDist),
          createdAt: {
            $gte: new Date(query.inicio),
            $lte: new Date(query.fin),
          },
        },
      },
      {
        $addFields: {
          fecha_group: {
            $dateToString: {
              format: "%Y-%m",
              date: "$createdAt",
            },
          },
          costoTotalProducto: {
            $multiply: ["$unidadesCompradas", "$costoProductos"],
          },
        },
      },
      {
        $facet: {
          clientes_mes: [
            {
              $group: {
                _id: "$idPedido",
                idPunto: { $first: "$idPunto" },
                fecha_group: { $first: "$fecha_group" },
              },
            },
            {
              $group: {
                _id: { idPunto: "$idPunto", fecha_group: "$fecha_group" },
                fecha_group: { $first: "$fecha_group" },
              },
            },
            {
              $group: {
                _id: "$fecha_group",
                total: { $sum: 1 },
              },
            },
            {
              $sort: { _id: 1 },
            },
          ],
        }
      }
    ]).exec(callback);
  },
  informesDistribuidorGraficasClientes: function (query, callback) {
    var ObjectId = require("mongoose").Types.ObjectId;
    this.aggregate([
      {
        $match: {
          idDistribuidor: new ObjectId(query.idDist)
        },
      },
      {
        $addFields: {
          fecha_group: {
            $dateToString: {
              format: "%Y-%m",
              date: "$createdAt",
            },
          },
          costoTotalProducto: {
            $multiply: ["$unidadesCompradas", "$costoProductos"],
          },
        },
      },
      {
        $facet: {
          clientes_estado: [
            {
              $group: {
                _id: "$idDistribuidor",
              },
            },
            {
              $lookup: {
                from: "distribuidores_vinculados",
                localField: "_id",
                foreignField: "distribuidor",
                as: "data_vinculacion",
              },
            },
            {
              $unwind: "$data_vinculacion",
            },
            {
              $group: {
                _id: "$data_vinculacion.estado",
                total: { $sum: 1 },
              },
            },
          ],
          clientes_tipo: [
            {
              $group: {
                _id: "$idDistribuidor",
              },
            },
            {
              $lookup: {
                from: "distribuidores_vinculados",
                localField: "_id",
                foreignField: "distribuidor",
                pipeline: [
                  {
                    $match: {
                      estado: "Aprobado",
                    },
                  },
                ],
                as: "data_vinculacion",
              },
            },
            {
              $unwind: "$data_vinculacion",
            },
            {
              $lookup: {
                from: "punto_entregas",
                localField:
                  "data_vinculacion.punto_entrega" /** Nombre del campo de la coleccion actual */,
                foreignField:
                  "_id" /** Nombre del campo de la coleccion a relacionar */,
                as: "data_punto" /** Nombre del campo donde se insertara todos los documentos relacionados */,
              },
            },
            {
              $lookup: {
                from: "usuario_horecas",
                localField:
                  "data_punto.usuario_horeca" /** Nombre del campo de la coleccion actual */,
                foreignField:
                  "_id" /** Nombre del campo de la coleccion a relacionar */,
                as: "data_horeca" /** Nombre del campo donde se insertara todos los documentos relacionados */,
              },
            },
            {
              $group: {
                _id: "$data_horeca.tipo_negocio",
                label: {
                  $first: { $arrayElemAt: ["$data_horeca.tipo_negocio", 0] },
                },
                total: { $sum: 1 },
              },
            },
            {
              $sort: { total: -1 },
            },
          ],
          clientes_tipo_usuario: [
            {
              $group: {
                _id: "$idDistribuidor",
              },
            },
            {
              $lookup: {
                from: "distribuidores_vinculados",
                localField: "_id",
                foreignField: "distribuidor",
                pipeline: [
                  {
                    $match: {
                      estado: "Aprobado",
                    },
                  },
                ],
                as: "data_vinculacion",
              },
            },
            {
              $unwind: "$data_vinculacion",
            },
            {
              $lookup: {
                from: "punto_entregas",
                localField:
                  "data_vinculacion.punto_entrega" /** Nombre del campo de la coleccion actual */,
                foreignField:
                  "_id" /** Nombre del campo de la coleccion a relacionar */,
                as: "data_punto" /** Nombre del campo donde se insertara todos los documentos relacionados */,
              },
            },
            {
              $lookup: {
                from: "usuario_horecas",
                localField:
                  "data_punto.usuario_horeca" /** Nombre del campo de la coleccion actual */,
                foreignField:
                  "_id" /** Nombre del campo de la coleccion a relacionar */,
                as: "data_horeca" /** Nombre del campo donde se insertara todos los documentos relacionados */,
              },
            },
            {
              $group: {
                _id: "$data_horeca.tipo_usuario",
                label: {
                  $first: { $arrayElemAt: ["$data_horeca.tipo_usuario", 0] },
                },
                total: { $sum: 1 },
              },
            },
            {
              $sort: { total: -1 },
            },
          ],
          clientes_sillas_x_negocio: [
            {
              $group: {
                _id: "$idDistribuidor",
              },
            },
            {
              $lookup: {
                from: "distribuidores_vinculados",
                localField: "_id",
                foreignField: "distribuidor",
                pipeline: [
                  {
                    $match: {
                      estado: "Aprobado",
                    },
                  },
                ],
                as: "data_vinculacion",
              },
            },
            {
              $unwind: "$data_vinculacion",
            },
            {
              $lookup: {
                from: "punto_entregas",
                localField:
                  "data_vinculacion.punto_entrega" /** Nombre del campo de la coleccion actual */,
                foreignField:
                  "_id" /** Nombre del campo de la coleccion a relacionar */,
                as: "data_punto" /** Nombre del campo donde se insertara todos los documentos relacionados */,
              },
            },
            {
              $lookup: {
                from: "usuario_horecas",
                localField:
                  "data_punto.usuario_horeca" /** Nombre del campo de la coleccion actual */,
                foreignField:
                  "_id" /** Nombre del campo de la coleccion a relacionar */,
                as: "data_horeca" /** Nombre del campo donde se insertara todos los documentos relacionados */,
              },
            },
            {
              $group: {
                _id: "$data_horeca.tipo_negocio",
                label: {
                  $first: { $arrayElemAt: ["$data_horeca.tipo_negocio", 0] },
                },
                total: {
                  $sum: { $arrayElemAt: ["$data_punto.sillas", 0] },
                },
              },
            },
            {
              $sort: { total: -1 },
            },
          ],
          clientes_domicilio: [
            {
              $group: {
                _id: "$idDistribuidor",
              },
            },
            {
              $lookup: {
                from: "distribuidores_vinculados",
                localField: "_id",
                foreignField: "distribuidor",
                pipeline: [
                  {
                    $match: {
                      estado: "Aprobado",
                    },
                  },
                ],
                as: "data_vinculacion",
              },
            },
            {
              $unwind: "$data_vinculacion",
            },
            {
              $lookup: {
                from: "punto_entregas",
                localField:
                  "data_vinculacion.punto_entrega" /** Nombre del campo de la coleccion actual */,
                foreignField:
                  "_id" /** Nombre del campo de la coleccion a relacionar */,
                as: "data_punto" /** Nombre del campo donde se insertara todos los documentos relacionados */,
              },
            },
            {
              $group: {
                _id: "$data_punto.domicilios",
                total: {
                  $sum: 1,
                },
              },
            },
          ],
          clientes_cartera: [
            {
              $group: {
                _id: "$idDistribuidor",
              },
            },
            {
              $lookup: {
                from: "distribuidores_vinculados",
                localField: "_id",
                foreignField: "distribuidor",
                pipeline: [
                  {
                    $match: {
                      estado: "Aprobado",
                    },
                  },
                ],
                as: "data_vinculacion",
              },
            },
            {
              $unwind: "$data_vinculacion",
            },
            {
              $group: {
                _id: "$data_vinculacion.cartera",
                total: {
                  $sum: 1,
                },
              },
            },
          ],
          clientes_convenio: [
            {
              $group: {
                _id: "$idDistribuidor",
              },
            },
            {
              $lookup: {
                from: "distribuidores_vinculados",
                localField: "_id",
                foreignField: "distribuidor",
                pipeline: [
                  {
                    $match: {
                      estado: "Aprobado",
                    },
                  },
                ],
                as: "data_vinculacion",
              },
            },
            {
              $unwind: "$data_vinculacion",
            },
            {
              $group: {
                _id: "$data_vinculacion.convenio",
                total: {
                  $sum: 1,
                },
              },
            },
          ],
          puntos_mapa: [
            {
              $group: {
                _id: "$idDistribuidor",
              },
            },
            {
              $lookup: {
                from: "distribuidores_vinculados",
                localField: "_id",
                foreignField: "distribuidor",
                pipeline: [
                  {
                    $match: {
                      estado: "Aprobado",
                    },
                  },
                  {
                    $lookup: {
                      from: "punto_entregas",
                      localField:
                        "punto_entrega" /** Nombre del campo de la coleccion actual */,
                      foreignField:
                        "_id" /** Nombre del campo de la coleccion a relacionar */,
                      as: "data_punto" /** Nombre del campo donde se insertara todos los documentos relacionados */,
                    },
                  },
                  {
                    $unwind: "$data_punto",
                  },
                  {
                    $replaceRoot: {
                      newRoot: "$data_punto",
                    },
                  },
                ],
                as: "data_vinculacion",
              },
            },
            {
              $unwind: "$data_vinculacion",
            },
            {
              $replaceRoot: {
                newRoot: "$data_vinculacion",
              },
            },
          ],
        },
      },
    ]).exec(callback);
  },
  informesDistribuidorGraficaSillasClientes: function (query, callback) {
    let ObjectId = require("mongoose").Types.ObjectId;
    this.aggregate([
      {
        $match: {
          idDistribuidor: new ObjectId(query.idDist),
        },
      },
      {
        $group: {
          _id: "$idDistribuidor",
        },
      },
      {
        $lookup: {
          from: "distribuidores_vinculados",
          localField: "_id",
          foreignField: "distribuidor",
          pipeline: [
            {
              $match: {
                estado: "Aprobado",
              },
            },
          ],
          as: "data_vinculacion",
        },
      },
      {
        $unwind: "$data_vinculacion",
      },
      {
        $lookup: {
          from: "punto_entregas",
          localField:
            "data_vinculacion.punto_entrega" /** Nombre del campo de la coleccion actual */,
          foreignField:
            "_id" /** Nombre del campo de la coleccion a relacionar */,
          as: "data_punto" /** Nombre del campo donde se insertara todos los documentos relacionados */,
        },
      },
      {
        $facet: {
          sillas0_10: [
            {
              $match: {
                "data_punto.sillas": {
                  $gte: 1,
                  $lte: 10,
                },
              },
            },
          ],
          sillas11_40: [
            {
              $match: {
                "data_punto.sillas": {
                  $gte: 11,
                  $lte: 40,
                },
              },
            },
          ],
          sillas41_80: [
            {
              $match: {
                "data_punto.sillas": {
                  $gte: 41,
                  $lte: 80,
                },
              },
            },
          ],
          sillas81_150: [
            {
              $match: {
                "data_punto.sillas": {
                  $gte: 81,
                  $lte: 151,
                },
              },
            },
          ],
          sillas151_300: [
            {
              $match: {
                "data_punto.sillas": {
                  $gte: 151,
                  $lte: 300,
                },
              },
            },
          ],
          sillas301_500: [
            {
              $match: {
                "data_punto.sillas": {
                  $gte: 301,
                  $lte: 500,
                },
              },
            },
          ],
          sillas501: [
            {
              $match: {
                "data_punto.sillas": {
                  $gte: 501,
                },
              },
            },
          ],
        },
      },
      {
        $project: {
          cant_sillas: [
            {
              label: "1-10",
              count: { $size: "$sillas0_10" },
            },
            {
              label: "11-40",
              count: { $size: "$sillas11_40" },
            },
            {
              label: "41-80",
              count: { $size: "$sillas41_80" },
            },
            {
              label: "81-150",
              count: { $size: "$sillas81_150" },
            },
            {
              label: "151-300",
              count: { $size: "$sillas151_300" },
            },
            {
              label: "301-500",
              count: { $size: "$sillas301_500" },
            },
            {
              label: "+500",
              count: { $size: "$sillas501" },
            },
          ],
        },
      },
    ]).exec(callback);
  },
  informesDistribuidorGraficaPuntosXEstab: function (query, callback) {
    let ObjectId = require("mongoose").Types.ObjectId;
    this.aggregate([
      {
        $match: {
          idDistribuidor: new ObjectId(query.idDist),
          // createdAt: {
          //   $gte: new Date(query.inicio),
          //   $lte: new Date(query.fin),
          // },
        },
      },
      {
        $group: {
          _id: "$idDistribuidor",
        },
      },
      {
        $lookup: {
          from: "distribuidores_vinculados",
          localField: "_id",
          foreignField: "distribuidor",
          pipeline: [
            {
              $match: {
                estado: "Aprobado",
              },
            },
          ],
          as: "data_vinculacion",
        },
      },
      {
        $unwind: "$data_vinculacion",
      },
      {
        $lookup: {
          from: "punto_entregas",
          localField:
            "data_vinculacion.punto_entrega" /** Nombre del campo de la coleccion actual */,
          foreignField:
            "_id" /** Nombre del campo de la coleccion a relacionar */,
          as: "data_punto" /** Nombre del campo donde se insertara todos los documentos relacionados */,
        },
      },
      {
        $unwind: "$data_punto",
      },
      {
        $replaceRoot: {
          newRoot: "$data_punto",
        },
      },
      {
        $group: {
          _id: "$usuario_horeca",
          total: {
            $sum: 1,
          },
        },
      },
      {
        $facet: {
          puntos1: [
            {
              $match: {
                total: 1,
              },
            },
          ],
          puntos2: [
            {
              $match: {
                total: 2,
              },
            },
          ],
          puntos3_5: [
            {
              $match: {
                total: {
                  $gte: 3,
                  $lte: 5,
                },
              },
            },
          ],
          puntos6_10: [
            {
              $match: {
                total: {
                  $gte: 6,
                  $lte: 10,
                },
              },
            },
          ],
          puntos11_20: [
            {
              $match: {
                total: {
                  $gte: 11,
                  $lte: 20,
                },
              },
            },
          ],
          puntos21: [
            {
              $match: {
                total: {
                  $gte: 21,
                },
              },
            },
          ],
        },
      },
      {
        $project: {
          puntos6_10: [
            {
              label: "6-10",
              total: { $size: "$puntos6_10" },
            },
          ],
          puntos11_20: [
            {
              label: "11-20",
              total: { $size: "$puntos11_20" },
            },
          ],
          puntos21: [
            {
              label: "+21",
              total: { $size: "$puntos21" },
            },
          ],
          puntos1: [
            {
              label: "1",
              total: { $size: "$puntos1" },
            },
          ],
          puntos2: [
            {
              label: "2",
              total: { $size: "$puntos2" },
            },
          ],
          puntos3_5: [
            {
              label: "3-5",
              total: { $size: "$puntos3_5" },
            },
          ],
        },
      },
    ]).exec(callback);
  },
  getTopVentasOrganizacion: function (query, callback) {
    var ObjectId = require("mongoose").Types.ObjectId;
    this.aggregate([
      {
        $match: { idOrganizacion: new ObjectId(query.idOrg) },
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
          _id: "$productoId",
          total: { $sum: "$costoTotalProducto" },
          logo: { $first: "$detalleProducto.fotos" },
          nombre: { $first: "$nombreProducto" },
          cod_org: { $first: "$codigo_organizacion_producto" },
        },
      },
      {
        $sort: { total: -1 },
      },
      {
        $limit: 10,
      },
    ]).exec(callback);
  },
  indicadoresDetallleProdOrg: function (query, callback) {
    var ObjectId = require("mongoose").Types.ObjectId;
    this.aggregate([
      {
        $match: { codigoFeatProducto: query.idCodigoFt },
      },
      {
        $addFields: {
          costoTotalProducto: {
            $multiply: ["$unidadesCompradas", "$costoProductos"], // Precio final
          },
        },
      },
      {
        $facet: {
          puntos: [
            {
              $group: {
                _id: "$idPunto",
                codigoFeatProducto: { $first: "$codigoFeatProducto" },
              },
            },
            {
              $group: {
                _id: "$codigoFeatProducto",
                total: { $sum: 1 },
              },
            },
          ],
          categoria: [
            {
              $group: {
                _id: "$codigoFeatProducto",
                total_mi_cat: { $sum: "$costoTotalProducto" },
                categoriaProducto: { $first: "$categoriaProducto" },
              },
            },
            {
              $lookup: {
                from: "reportepedidos",
                localField: "categoriaProducto", //Nombre del campo de la coleccion actual
                foreignField: "categoriaProducto", //Nombre del campo de la coleccion a relacionar
                pipeline: [
                  {
                    $addFields: {
                      costoTotalProducto: {
                        $multiply: ["$unidadesCompradas", "$costoProductos"], // Precio final
                      },
                    },
                  },
                  {
                    $group: {
                      _id: "$categoriaProducto",
                      total_todas_cat: { $sum: "$costoTotalProducto" },
                    },
                  },
                ],
                as: "data_total_cat", //Nombre del campo donde se insertara todos los documentos relacionados
              },
            },
            {
              $unwind: "$data_total_cat",
            },
            {
              $project: {
                participacion: {
                  $divide: ["$total_mi_cat", "$data_total_cat.total_todas_cat"],
                },
              },
            },
          ],
        },
      },
      {
        $project: {
          puntos_entrega: { $arrayElemAt: ["$puntos.total", 0] },
          participacion_cat: { $arrayElemAt: ["$categoria.participacion", 0] },
        },
      },
    ]).exec(callback);
  },
  distHomeEstablecimientosXCategorias: function (query, callback) {
    var ObjectId = require("mongoose").Types.ObjectId;
    this.aggregate([
      {
        $match: { idDistribuidor: new ObjectId(query) },
      },
      {
        $lookup: {
          from: "pedidos",
          localField: "idPedido", //Nombre del campo de la coleccion actual
          foreignField: "_id", //Nombre del campo de la coleccion a relacionar
          pipeline: [
            {
              $project: {
                estado: "$estado",
              },
            },
          ],
          as: "pedido_estado", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
      {
        $unwind: "$pedido_estado",
      },
      {
        $match: {
          $or: [
            { "pedido_estado.estado": "Entregado" },
            { "pedido_estado.estado": "Recibido" },
            { "pedido_estado.estado": "Calificado" },
          ],
        },
      },
      {
        $group: {
          _id: {
            idPunto: "$idPunto",
            categoriaProducto: "$categoriaProducto",
          },
          categoriaProducto: { $first: "$categoriaProducto" },
        },
      },
      {
        $group: {
          _id: "$categoriaProducto",
          total: { $sum: 1 },
        },
      },
      /** Nombre categoria producto */
      {
        $lookup: {
          from: "categoria_productos", //Nombre de la colecccion a relacionar
          localField: "_id", //Nombre del campo de la coleccion actual
          foreignField: "_id", //Nombre del campo de la coleccion a relacionar
          pipeline: [
            {
              $project: {
                nombre: "$nombre",
              },
            },
          ],
          as: "categoria_producto", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
      {
        $unwind: "$categoria_producto",
      },
      {
        $project: {
          nombre: "$categoria_producto.nombre",
          total: "$total",
        },
      },
      {
        $sort: { total: -1 },
      },
    ]).exec(callback);
  },
  distHomeSillassXCategorias: function (query, callback) {
    var ObjectId = require("mongoose").Types.ObjectId;
    this.aggregate([
      {
        $match: { idDistribuidor: new ObjectId(query) },
      },
      {
        $lookup: {
          from: "pedidos",
          localField: "idPedido", //Nombre del campo de la coleccion actual
          foreignField: "_id", //Nombre del campo de la coleccion a relacionar
          pipeline: [
            {
              $project: {
                estado: "$estado",
              },
            },
          ],
          as: "pedido_estado", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
      {
        $unwind: "$pedido_estado",
      },
      {
        $match: {
          $or: [
            { "pedido_estado.estado": "Entregado" },
            { "pedido_estado.estado": "Recibido" },
            { "pedido_estado.estado": "Calificado" },
          ],
        },
      },
      {
        $group: {
          _id: {
            categoriaProducto: "$categoriaProducto",
            idPunto: "$idPunto",
            sillas: "$puntoSillas"
          }
        },
      },
      {
        $lookup: {
          from: "categoria_productos", //Nombre de la colecccion a relacionar
          localField: "_id.categoriaProducto", //Nombre del campo de la coleccion actual
          foreignField: "_id", //Nombre del campo de la coleccion a relacionar
          pipeline: [
            {
              $project: {
                nombre: "$nombre",
              },
            },
          ],
          as: "categoria_producto", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
      {
        $unwind: "$categoria_producto"
      },
      {
        $group: {
          _id: "$categoria_producto.nombre",
          total: { $sum: "$_id.sillas" }
        }
      },
      {
        $project: {
          label: "$_id",
          count: "$total"
        }
      },
      {
        $sort: { count: -1 },
      },
    ]).exec(callback);
  },
  /***************** Sector DISTRIBUIDOR ****************/
  getSectorDistVentas: function (query, ciudad, callback) {
    let ObjectId = require("mongoose").Types.ObjectId;
    if (ciudad.ciudad === "todos") {
      this.aggregate([
        /** Arma la data de la grafica */
        {
          $addFields: {
            costoTotalProducto: {
              $multiply: ["$unidadesCompradas", "$costoProductos"],
            },
          },
        },
        {
          $facet: {
            miOrganizacion: [
              {
                $match: {
                  idDistribuidor: new ObjectId(query.idDist),
                },
              },
            ],
            otrasOrganizaciones: [
              {
                $match: {},
              },
            ],
          },
        },
        {
          $project: {
            otrasOrganizaciones: [
              {
                label: "Otras organizaciones",
                total: { $sum: "$otrasOrganizaciones.costoTotalProducto" },
              },
            ],
            miOrganizacion: [
              {
                label: "Organización",
                total: { $sum: "$miOrganizacion.costoTotalProducto" },
              },
            ],
          },
        },
      ]).exec(callback);
    } else {
      this.aggregate([
        /** Recupera la ciudad del punto y aplica el filtro de ciudad */
        {
          $lookup: {
            from: "punto_entregas",
            localField: "idPunto",
            foreignField: "_id",
            pipeline: [
              {
                $project: {
                  ciudad: "$ciudad",
                },
              },
            ],
            as: "dataPunto",
          },
        },
        {
          $addFields: {
            dataPunto_: { $arrayElemAt: ["$dataPunto", 0] },
          },
        },
        {
          $addFields: {
            punto_ciudad: "$dataPunto_.ciudad",
          },
        },
        {
          $match: {
            punto_ciudad: ciudad.ciudad,
          },
        },
        /** Arma la data de la grafica */
        {
          $addFields: {
            costoTotalProducto: {
              $multiply: ["$unidadesCompradas", "$costoProductos"],
            },
          },
        },
        {
          $facet: {
            miOrganizacion: [
              {
                $match: {
                  idDistribuidor: new ObjectId(query.idDist),
                },
              },
            ],
            otrasOrganizaciones: [
              {
                $match: {},
              },
            ],
          },
        },
        {
          $project: {
            miOrganizacion: [
              {
                label: "Organización",
                total: { $sum: "$miOrganizacion.costoTotalProducto" },
              },
            ],
            otrasOrganizaciones: [
              {
                label: "Otras organizaciones",
                total: { $sum: "$otrasOrganizaciones.costoTotalProducto" },
              },
            ],
          },
        },
      ]).exec(callback);
    }
  },
  getSectorDistClientes: function (query, ciudad, callback) {
    let ObjectId = require("mongoose").Types.ObjectId;
    if (ciudad.ciudad !== "todos") {
      this.aggregate([
        /** Recupera la ciudad del punto y aplica el filtro de ciudad */
        {
          $lookup: {
            from: "punto_entregas",
            localField: "idPunto", //Nombre del campo de la coleccion actual
            foreignField: "_id", //Nombre del campo de la coleccion a relacionar
            pipeline: [
              {
                $project: {
                  ciudad: "$ciudad",
                },
              },
            ],
            as: "dataPunto", //Nombre del campo donde se insertara todos los documentos relacionados
          },
        },
        {
          $addFields: {
            dataPunto_: { $arrayElemAt: ["$dataPunto", 0] },
          },
        },
        {
          $addFields: {
            punto_ciudad: "$dataPunto_.ciudad",
          },
        },
        {
          $match: {
            punto_ciudad: ciudad.ciudad,
          },
        },
        {
          $group: {
            _id: {
              idDistribuidor: "$idDistribuidor",
              idPunto: "$idPunto",
            },
            total: { $sum: 1 },
            idPunto: { $first: "$idPunto" },
            idDistribuidor: { $first: "$idDistribuidor" },
          },
        },
        /** Arma la data de la grafica */
        {
          $facet: {
            otrasOrganizaciones: [
              {
                $match: {},
              },
            ],
            miOrganizacion: [
              {
                $match: {
                  idDistribuidor: new ObjectId(query.idDist),
                },
              },
            ],
          },
        },
        {
          $project: {
            miOrganizacion: [
              {
                label: "Organización",
                total: { $size: "$miOrganizacion" },
              },
            ],
            otrasOrganizaciones: [
              {
                label: "Otras organizaciones",
                total: { $size: "$otrasOrganizaciones" },
              },
            ],
          },
        },
      ]).exec(callback);
    } else {
      this.aggregate([
        {
          $group: {
            _id: {
              idDistribuidor: "$idDistribuidor",
              idPunto: "$idPunto",
            },
            total: { $sum: 1 },
            idPunto: { $first: "$idPunto" },
            idDistribuidor: { $first: "$idDistribuidor" },
          },
        },
        /** Arma la data de la grafica */
        {
          $facet: {
            miOrganizacion: [
              {
                $match: {
                  idDistribuidor: new ObjectId(query.idDist),
                },
              },
            ],
            otrasOrganizaciones: [
              {
                $match: {},
              },
            ],
          },
        },
        {
          $project: {
            otrasOrganizaciones: [
              {
                label: "Otras organizaciones",
                total: { $size: "$otrasOrganizaciones" },
              },
            ],
            miOrganizacion: [
              {
                label: "Organización",
                total: { $size: "$miOrganizacion" },
              },
            ],
          },
        },
      ]).exec(callback);
    }
  },
  getSectorDistSillas: function (query, ciudad, callback) {
    let ObjectId = require("mongoose").Types.ObjectId;
    if (ciudad.ciudad === "todos") {
      this.aggregate([
        {
          $lookup: {
            from: "punto_entregas",
            localField: "idPunto", //Nombre del campo de la coleccion actual
            foreignField: "_id", //Nombre del campo de la coleccion a relacionar
            pipeline: [
              {
                $project: {
                  sillas: "$sillas",
                  ciudad: "$ciudad",
                },
              },
            ],
            as: "dataPunto", //Nombre del campo donde se insertara todos los documentos relacionados
          },
        },
        {
          $addFields: {
            dataPunto_: { $arrayElemAt: ["$dataPunto", 0] },
          },
        },
        {
          $addFields: {
            punto_ciudad: "$dataPunto_.ciudad",
            punto_sillas: "$dataPunto_.sillas",
          },
        },
        {
          $group: {
            _id: {
              idDistribuidor: "$idDistribuidor",
              idPunto: "$idPunto",
            },
            total: { $sum: "$punto_sillas" },
            idPunto: { $first: "$idPunto" },
            punto_sillas: { $first: "$punto_sillas" },
            idDistribuidor: { $first: "$idDistribuidor" },
          },
        },
        /** Arma la data de la grafica */
        {
          $facet: {
            miOrganizacion: [
              {
                $match: {
                  idDistribuidor: new ObjectId(query.idDist),
                },
              },
            ],
            otrasOrganizaciones: [
              {
                $match: {},
              },
            ],
          },
        },
        {
          $project: {
            miOrganizacion: [
              {
                label: "Organización",
                total: { $sum: "$miOrganizacion.punto_sillas" },
              },
            ],

            otrasOrganizaciones: [
              {
                label: "Otras organizaciones",
                total: { $sum: "$otrasOrganizaciones.punto_sillas" },
              },
            ],
          },
        },
      ]).exec(callback);
    } else {
      this.aggregate([
        /** Recupera la ciudad del punto y aplica el filtro de ciudad */
        {
          $lookup: {
            from: "punto_entregas",
            localField: "idPunto", //Nombre del campo de la coleccion actual
            foreignField: "_id", //Nombre del campo de la coleccion a relacionar
            pipeline: [
              {
                $project: {
                  sillas: "$sillas",
                  ciudad: "$ciudad",
                },
              },
            ],
            as: "dataPunto", //Nombre del campo donde se insertara todos los documentos relacionados
          },
        },
        {
          $addFields: {
            dataPunto_: { $arrayElemAt: ["$dataPunto", 0] },
          },
        },
        {
          $addFields: {
            punto_ciudad: "$dataPunto_.ciudad",
            punto_sillas: "$dataPunto_.sillas",
          },
        },
        {
          $match: {
            punto_ciudad: ciudad.ciudad,
          },
        },
        {
          $group: {
            _id: {
              idDistribuidor: "$idDistribuidor",
              idPunto: "$idPunto",
            },
            total: { $sum: "$punto_sillas" },
            idPunto: { $first: "$idPunto" },
            punto_sillas: { $first: "$punto_sillas" },
            idDistribuidor: { $first: "$idDistribuidor" },
          },
        },
        /** Arma la data de la grafica */
        {
          $facet: {
            otrasOrganizaciones: [
              {
                $match: {},
              },
            ],
            miOrganizacion: [
              {
                $match: {
                  idDistribuidor: new ObjectId(query.idDist),
                },
              },
            ],
          },
        },
        {
          $project: {
            otrasOrganizaciones: [
              {
                label: "Otras organizaciones",
                total: { $sum: "$otrasOrganizaciones.punto_sillas" },
              },
            ],
            miOrganizacion: [
              {
                label: "Organización",
                total: { $sum: "$miOrganizacion.punto_sillas" },
              },
            ],
          },
        },
      ]).exec(callback);
    }
  },
  getSectorDistCategorias: function (query, callback) {
    let ObjectId = require("mongoose").Types.ObjectId;
    if (query.idCat === "todos") {
      this.aggregate([
        /** Arma la data de la grafica */
        {
          $addFields: {
            costoTotalProducto: {
              $multiply: ["$unidadesCompradas", "$costoProductos"],
            },
          },
        },
        /** Arma la data de la grafica */
        {
          $facet: {
            miOrganizacion: [
              {
                $match: {
                  idDistribuidor: new ObjectId(query.idDist),
                },
              },
            ],
            otrasOrganizaciones: [
              {
                $match: {},
              },
            ],
          },
        },
        {
          $project: {
            miOrganizacion: [
              {
                label: "Organización",
                total: { $sum: "$miOrganizacion.costoTotalProducto" },
              },
            ],
            otrasOrganizaciones: [
              {
                label: "Otras organizaciones",
                total: { $sum: "$otrasOrganizaciones.costoTotalProducto" },
              },
            ],
          },
        },
      ]).exec(callback);
    } else {
      this.aggregate([
        {
          $match: {
            categoriaProducto: new ObjectId(query.idCat),
          },
        },
        /** Arma la data de la grafica */
        {
          $addFields: {
            costoTotalProducto: {
              $multiply: ["$unidadesCompradas", "$costoProductos"],
            },
          },
        },
        /** Arma la data de la grafica */
        {
          $facet: {
            miOrganizacion: [
              {
                $match: {
                  idDistribuidor: new ObjectId(query.idDist),
                },
              },
            ],
            otrasOrganizaciones: [
              {
                $match: {},
              },
            ],
          },
        },
        {
          $project: {
            miOrganizacion: [
              {
                label: "Organización",
                total: { $sum: "$miOrganizacion.costoTotalProducto" },
              },
            ],
            otrasOrganizaciones: [
              {
                label: "Otras organizaciones",
                total: { $sum: "$otrasOrganizaciones.costoTotalProducto" },
              },
            ],
          },
        },
      ]).exec(callback);
    }
  },
  getSectorDistLineas: function (query, callback) {
    let ObjectId = require("mongoose").Types.ObjectId;
    if (query.idLinea === "todos") {
      this.aggregate([
        {
          $addFields: {
            linea_producto: {
              $arrayElemAt: ["$detalleProducto.linea_producto", 0],
            },
          },
        },
        /** Arma la data de la grafica */
        {
          $addFields: {
            costoTotalProducto: {
              $multiply: ["$unidadesCompradas", "$costoProductos"],
            },
          },
        },
        /** Arma la data de la grafica */
        {
          $facet: {
            miOrganizacion: [
              {
                $match: {
                  idDistribuidor: new ObjectId(query.idDist),
                },
              },
            ],
            otrasOrganizaciones: [
              {
                $match: {},
              },
            ],
          },
        },
        {
          $project: {
            miOrganizacion: [
              {
                label: "Organización",
                total: { $sum: "$miOrganizacion.costoTotalProducto" },
              },
            ],
            otrasOrganizaciones: [
              {
                label: "Otras organizaciones",
                total: { $sum: "$otrasOrganizaciones.costoTotalProducto" },
              },
            ],
          },
        },
      ]).exec(callback);
    } else {
      this.aggregate([
        {
          $addFields: {
            linea_producto: {
              $arrayElemAt: ["$detalleProducto.linea_producto", 0],
            },
          },
        },
        {
          $match: {
            linea_producto: query.idLinea,
          },
        },
        /** Arma la data de la grafica */
        {
          $addFields: {
            costoTotalProducto: {
              $multiply: ["$unidadesCompradas", "$costoProductos"],
            },
          },
        },
        {
          $facet: {
            miOrganizacion: [
              {
                $match: {
                  idDistribuidor: new ObjectId(query.idDist),
                },
              },
            ],
            otrasOrganizaciones: [
              {
                $match: {},
              },
            ],
          },
        },
        {
          $project: {
            miOrganizacion: [
              {
                label: "Organización",
                total: { $sum: "$miOrganizacion.costoTotalProducto" },
              },
            ],
            otrasOrganizaciones: [
              {
                label: "Otras organizaciones",
                total: { $sum: "$otrasOrganizaciones.costoTotalProducto" },
              },
            ],
          },
        },
      ]).exec(callback);
    }
  },
  getSectorDistTipoNegocio: function (query, tipo_negocio, callback) {
    let ObjectId = require("mongoose").Types.ObjectId;
    if (tipo_negocio.tipo_negocio === "todos") {
      this.aggregate([
        {
          $addFields: {
            costoTotalProducto: {
              $multiply: ["$unidadesCompradas", "$costoProductos"],
            },
          },
        },
        /** Recupera el tipo de negocio, la tabla secundaria no lo tiene */
        {
          $lookup: {
            from: "punto_entregas",
            localField: "idPunto", //Nombre del campo de la coleccion actual
            foreignField: "_id", //Nombre del campo de la coleccion a relacionar
            pipeline: [
              {
                $project: {
                  usuario_horeca: "$usuario_horeca",
                },
              },
              {
                $lookup: {
                  from: "usuario_horecas",
                  localField: "usuario_horeca", //Nombre del campo de la coleccion actual
                  foreignField: "_id", //Nombre del campo de la coleccion a relacionar
                  pipeline: [
                    {
                      $project: {
                        tipo_negocio: "$tipo_negocio",
                      },
                    },
                  ],
                  as: "datahoreca", //Nombre del campo donde se insertara todos los documentos relacionados
                },
              },
              {
                $addFields: {
                  datahoreca_: { $arrayElemAt: ["$datahoreca", 0] },
                },
              },
            ],
            as: "dataPunto", //Nombre del campo donde se insertara todos los documentos relacionados
          },
        },
        {
          $addFields: {
            tipo_negocio_: {
              $arrayElemAt: ["$dataPunto.datahoreca_.tipo_negocio", 0],
            },
          },
        },
        {
          $project: {
            tipo_negocio_: "$tipo_negocio_",
            costoTotalProducto: "$costoTotalProducto",
            idDistribuidor: "$idDistribuidor",
          },
        },
        /** Arma la data de la grafica */
        {
          $facet: {
            miOrganizacion: [
              {
                $match: {
                  idDistribuidor: new ObjectId(query.idDist),
                },
              },
            ],
            otrasOrganizaciones: [
              {
                $match: {},
              },
            ],
          },
        },
        {
          $project: {
            miOrganizacion: [
              {
                label: "Organización",
                total: { $sum: "$miOrganizacion.costoTotalProducto" },
              },
            ],
            otrasOrganizaciones: [
              {
                label: "Otras organizaciones",
                total: { $sum: "$otrasOrganizaciones.costoTotalProducto" },
              },
            ],
          },
        },
      ]).exec(callback);
    } else {
      this.aggregate([
        {
          $addFields: {
            costoTotalProducto: {
              $multiply: ["$unidadesCompradas", "$costoProductos"],
            },
          },
        },
        /** Recupera el tipo de negocio, la tabla secundaria no lo tiene */
        {
          $lookup: {
            from: "punto_entregas",
            localField: "idPunto", //Nombre del campo de la coleccion actual
            foreignField: "_id", //Nombre del campo de la coleccion a relacionar
            pipeline: [
              {
                $project: {
                  usuario_horeca: "$usuario_horeca",
                },
              },
              {
                $lookup: {
                  from: "usuario_horecas",
                  localField: "usuario_horeca", //Nombre del campo de la coleccion actual
                  foreignField: "_id", //Nombre del campo de la coleccion a relacionar
                  pipeline: [
                    {
                      $project: {
                        tipo_negocio: "$tipo_negocio",
                      },
                    },
                  ],
                  as: "datahoreca", //Nombre del campo donde se insertara todos los documentos relacionados
                },
              },
              {
                $addFields: {
                  datahoreca_: { $arrayElemAt: ["$datahoreca", 0] },
                },
              },
            ],
            as: "dataPunto", //Nombre del campo donde se insertara todos los documentos relacionados
          },
        },
        {
          $addFields: {
            tipo_negocio_: {
              $arrayElemAt: ["$dataPunto.datahoreca_.tipo_negocio", 0],
            },
          },
        },
        {
          $project: {
            tipo_negocio_: "$tipo_negocio_",
            costoTotalProducto: "$costoTotalProducto",
            idDistribuidor: "$idDistribuidor",
          },
        },
        /** Aplica el filtro de tipo de negocio */
        {
          $match: {
            tipo_negocio_: tipo_negocio.tipo_negocio,
          },
        },
        /** Arma la data de la grafica */
        {
          $facet: {
            miOrganizacion: [
              {
                $match: {
                  idDistribuidor: new ObjectId(query.idDist),
                },
              },
            ],
            otrasOrganizaciones: [
              {
                $match: {},
              },
            ],
          },
        },
        {
          $project: {
            miOrganizacion: [
              {
                label: "Organización",
                total: { $sum: "$miOrganizacion.costoTotalProducto" },
              },
            ],
            otrasOrganizaciones: [
              {
                label: "Otras organizaciones",
                total: { $sum: "$otrasOrganizaciones.costoTotalProducto" },
              },
            ],
          },
        },
      ]).exec(callback);
    }
  },
  distribuidoresSector: function (query, callback) {
    let ObjectId = require("mongoose").Types.ObjectId;
    if (query.idDist === "todos") {
      this.aggregate([
        /** Arma la data de la grafica */
        {
          $addFields: {
            costoTotalProducto: {
              $multiply: ["$unidadesCompradas", "$costoProductos"],
            },
          },
        },
        /** Arma la data de la grafica */
        {
          $facet: {
            miOrganizacion: [
              {
                $match: {
                  idOrganizacion: new ObjectId(query.idOrg),
                },
              },
            ],
            otrasOrganizaciones: [
              {
                $match: {},
              },
            ],
          },
        },
        {
          $project: {
            miOrganizacion: [
              {
                label: "Organización",
                total: { $sum: "$miOrganizacion.costoTotalProducto" },
              },
            ],
            otrasOrganizaciones: [
              {
                label: "Otras organizaciones",
                total: { $sum: "$otrasOrganizaciones.costoTotalProducto" },
              },
            ],
          },
        },
      ]).exec(callback);
    } else {
      this.aggregate([
        {
          $match: {
            idDistribuidor: new ObjectId(query.idDist),
          },
        },
        /** Arma la data de la grafica */
        {
          $addFields: {
            costoTotalProducto: {
              $multiply: ["$unidadesCompradas", "$costoProductos"],
            },
          },
        },
        /** Nuevos campos */
        {
          $facet: {
            miOrganizacion: [
              {
                $match: {
                  idOrganizacion: new ObjectId(query.idOrg),
                },
              },
            ],
            otrasOrganizaciones: [
              {
                $match: {},
              },
            ],
          },
        },
        /** */
        {
          $project: {
            otrasOrganizaciones: [
              {
                label: "Otras organizaciones",
                total: { $sum: "$otrasOrganizaciones.costoTotalProducto" },
              },
            ],
            miOrganizacion: [
              {
                label: "Organización",
                total: { $sum: "$miOrganizacion.costoTotalProducto" },
              },
            ],
          },
        },
      ]).exec(callback);
    }
  },
  /** Distribuidores de una organizacion */
  informedistribuidorPuntos: function (query, callback) {
    let ObjectId = require("mongoose").Types.ObjectId;
    this.aggregate([
      /** Arma la data de la grafica */
      {
        $match: {
          idDistribuidor: new ObjectId(query),
          descuento: { $gt: 0 }
        },
      },
      {
        $group: {
          _id: "$idPedido",
          puntos_ft_ganados: { $first: "$puntosGanados" },
          idUserHoreca: { $first: "$idUserHoreca" },
          puntoEntrega: { $first: "$idPunto" },
          pedido: { $first: "$idPedido" },
          fecha: {
            $first: {
              $dateToString: {
                format: "%Y-%m-%d",
                date: "$createdAt",
              },
            },
          },
        },
      },
      {
        $lookup: {
          from: "usuario_horecas",
          localField: "idUserHoreca", //Nombre del campo de la coleccion actual
          foreignField: "_id", //Nombre del campo de la coleccion a relacionar
          as: "datahoreca", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
      {
        $lookup: {
          from: "punto_entregas",
          localField: "puntoEntrega", //Nombre del campo de la coleccion actual
          foreignField: "_id", //Nombre del campo de la coleccion a relacionar
          as: "dataPuntoEntrega", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
      {
        $lookup: {
          from: "pedidos",
          localField: "pedido", //Nombre del campo de la coleccion actual
          foreignField: "_id", //Nombre del campo de la coleccion a relacionar
          as: "dataPedido", //Nombre del campo donde se insertara todos los documentos relacionados
          pipeline: [
            {
              $lookup: {
                from: "codigos_descuento_generados",
                localField: "codigo_descuento", //Nombre del campo de la coleccion actual
                foreignField: "_id", //Nombre del campo de la coleccion a relacionar
                as: "dataCodigo", //Nombre del campo donde se insertara todos los documentos relacionados
              },
            },
          ],
        },
      },
      {
        $sort: { fecha: -1 },
      },
      {
        $project: {
          puntos_ganados: "$puntos_ft_ganados",
          fecha: "$fecha",
          idPedido: "$dataPedido.id_pedido",
          list_code: "$dataPedido.dataCodigo",
          codigoRedencion: { $first: "$dataPedido.dataCodigo.codigo_creado" },
          puntoEntrega: { $first: "$dataPuntoEntrega.nombre" },
          establecimiento: { $first: "$datahoreca.nombre_establecimiento" },
          nit_establecimiento: { $first: "$datahoreca.nit" },
          tipo_persona_establecimiento: { $first: "$datahoreca.tipo_usuario" },
          tipo_negocio_establecimiento: { $first: "$datahoreca.tipo_negocio" },
          valor_pedido: "$dataPedido.subtotal_pedido"
        },
      },
    ]).exec(callback);
  },
  informePuntosPorMesDistribuidor: function (query, callback) {
    let ObjectId = require("mongoose").Types.ObjectId;
    this.aggregate([
      {
        $match: {
          idDistribuidor: new ObjectId(query.idDistribuidor),
          descuento: { $gt: 0 }
        },
      },
      {
        $group: {
          _id: "$idPedido",
          puntos_ft_ganados: { $first: "$puntosGanados" },
          idUserHoreca: { $first: "$idUserHoreca" },
          puntoEntrega: { $first: "$idPunto" },
          pedido: { $first: "$idPedido" },
          fecha: {
            $first: "$createdAt",
          },
        },
      },
      {
        $lookup: {
          from: "pedidos",
          localField: "pedido",
          foreignField: "_id",
          as: "dataPedido",
          pipeline: [
            {
              $lookup: {
                from: "codigos_descuento_generados",
                localField: "codigo_descuento",
                foreignField: "_id",
                as: "dataCodigo",
              },
            },
            {
              $unwind: '$dataCodigo'
            }
          ],
        },
      },
      {
        $unwind: '$dataPedido'
      },
      {
        $replaceRoot: {
          newRoot: "$dataPedido",
        },
      },
      {
        $match: {
          createdAt: {
            $gte: new Date(query.inicio),
            $lte: new Date(query.fin),
          }
        }
      },
      {
        $addFields: {
          puntosFt_fecha: {
            $dateToString: {
              format: "%Y-%m",
              date: "$createdAt",
            },
          },
        },
      },
      {
        $group: {
          _id: "$puntosFt_fecha",
          total: { $sum: "$dataCodigo.valor_paquete" },
        },
      },
      {
        $sort: { _id: 1 },
      }
    ]).exec(callback);
  },
  informePuntosFtEstablecimientosDistribuidor: function (query, callback) {
    let ObjectId = require("mongoose").Types.ObjectId;
    this.aggregate([
      {
        $match: {
          idDistribuidor: new ObjectId(query.idDistribuidor),
          descuento: { $gt: 0 }
        },
      },
      {
        $group: {
          _id: "$idPedido",
          puntos_ft_ganados: { $first: "$puntosGanados" },
          idUserHoreca: { $first: "$idUserHoreca" },
          puntoEntrega: { $first: "$idPunto" },
          pedido: { $first: "$idPedido" }
        },
      },
      {
        $lookup: {
          from: "pedidos",
          localField: "pedido",
          foreignField: "_id",
          as: "dataPedido",
          pipeline: [
            {
              $lookup: {
                from: "codigos_descuento_generados",
                localField: "codigo_descuento",
                foreignField: "_id",
                as: "dataCodigo",
              },
            },
            {
              $unwind: '$dataCodigo'
            }
          ],
        },
      },
      {
        $unwind: '$dataPedido'
      },
      {
        $replaceRoot: {
          newRoot: "$dataPedido",
        },
      },
      {
        $match: {
          createdAt: {
            $gte: new Date(query.inicio),
            $lte: new Date(query.fin),
          }
        }
      },
      {
        $addFields: {
          puntosFt_fecha: {
            $dateToString: {
              format: "%Y-%m",
              date: "$createdAt",
            },
          },
        },
      },
      {
        $group: {
          _id: {
            puntosFt_fecha: "$puntosFt_fecha",
            punto_entrega: "$punto_entrega",
          },
          puntosFt_fecha: { $first: "$puntosFt_fecha" },
          total: { $sum: 1 },
        },
      },
      {
        $group: {
          _id: "$puntosFt_fecha",
          total: { $sum: 1 },
        },
      },
      {
        $sort: { _id: 1 },
      },
    ]).exec(callback);
  },
  informeCodigosRedencionDistribuidor: function (query, callback) {
    let ObjectId = require("mongoose").Types.ObjectId;
    this.aggregate([
      {
        $match: {
          idDistribuidor: new ObjectId(query.idDistribuidor),
          descuento: { $gt: 0 }
        },
      },
      {
        $group: {
          _id: "$idPedido",
          puntos_ft_ganados: { $first: "$puntosGanados" },
          idUserHoreca: { $first: "$idUserHoreca" },
          puntoEntrega: { $first: "$idPunto" },
          pedido: { $first: "$idPedido" }
        },
      },
      {
        $lookup: {
          from: "pedidos",
          localField: "pedido",
          foreignField: "_id",
          as: "dataPedido",
          pipeline: [
            {
              $lookup: {
                from: "codigos_descuento_generados",
                localField: "codigo_descuento",
                foreignField: "_id",
                as: "dataCodigo",
              },
            },
            {
              $unwind: '$dataCodigo'
            }
          ],
        },
      },
      {
        $unwind: '$dataPedido'
      },
      {
        $replaceRoot: {
          newRoot: "$dataPedido",
        },
      },
      {
        $match: {
          createdAt: {
            $gte: new Date(query.inicio),
            $lte: new Date(query.fin),
          }
        }
      },
      {
        $addFields: {
          puntosFt_fecha: {
            $dateToString: {
              format: "%Y-%m",
              date: "$createdAt",
            },
          },
        },
      },
      {
        $group: {
          _id: {
            codigo_creado: "$dataCodigo.codigo_creado",
            punto_entrega: "$punto_entrega",
          },
          puntosFt_fecha: { $first: "$puntosFt_fecha" },
          total: { $sum: 1 },
        },
      },
      {
        $group: {
          _id: "$puntosFt_fecha",
          total: { $sum: 1 },
        },
      },
      {
        $sort: { _id: 1 },
      }
    ]).exec(callback);
  },
  /** Trae toda la informacion del componente de puntos-feat de una organizacion */
  getDataComponentePuntosFeatNew: function (query, callback) {
    var ObjectId = require("mongoose").Types.ObjectId;
    this.aggregate([
      {
        $match: {
          idOrganizacion: new ObjectId(query.idOrg),
          createdAt: {
            $gte: new Date(query.inicio),
            $lte: new Date(query.fin),
          },
          puntos_ft_ganados: {
            $gt: 0,
          }
        },

      },
      {
        $lookup: {
          from: "productos",
          localField: "productoId",
          foreignField: "_id",
          as: "data_prod",
        },
      },
      {
        $facet: {
          producto: [
            {
              $project: {
                idProducto: '$productoId',
                producto: { $arrayElemAt: ['$data_prod.nombre', 0] },
                puntosFt_asignados: '$puntos_ft_unidad',
                puntosFt_ganados: '$puntos_ft_ganados',
                puntosFt_fecha_inicio: { $arrayElemAt: ['$data_prod.fecha_apertura_puntosft', 0] },
                puntosFt_fecha_cierre: { $arrayElemAt: ['$data_prod.fecha_cierre_puntosft', 0] },
                marca: '$marcaProducto',
                categoria: '$categoriaProducto',
                fechaped: '$createdAt'

              }
            },
          ],
          marca: [
            {
              $project: {
                idProducto: '$productoId',
                nombre: { $arrayElemAt: ['$data_prod.nombre', 0] },
                puntosUnidad: '$puntos_ft_unidad',
                puntosFt_ganados: '$puntos_ft_ganados',
                fechaInicio: { $arrayElemAt: ['$data_prod.fecha_apertura_puntosft', 0] },
                fechaFin: { $arrayElemAt: ['$data_prod.fecha_cierre_puntosft', 0] },
                marca: '$marcaProducto',
                categoria: '$categoriaProducto',
                fechaped: '$createdAt'

              }
            },
            {
              $lookup: {
                from: "marca_productos",
                localField: "marca",
                foreignField: "_id",
                as: "data_marca",
              },
            },
            {
              $group: {
                _id: "$marca",
                marca: { $last: "$data_marca.nombre" },
                puntosFt_asignados: { $sum: "$puntosUnidad" },
                puntosFt_ganados: { $sum: "$puntosFt_ganados" },
              },
            },

          ],
          categoria: [
            {
              $project: {
                idProducto: '$productoId',
                nombre: { $arrayElemAt: ['$data_prod.nombre', 0] },
                puntosUnidad: '$puntos_ft_unidad',
                puntosFt_ganados: '$puntos_ft_ganados',
                fechaInicio: { $arrayElemAt: ['$data_prod.fecha_apertura_puntosft', 0] },
                fechaFin: { $arrayElemAt: ['$data_prod.fecha_cierre_puntosft', 0] },
                marca: '$marcaProducto',
                categoria: '$categoriaProducto',
                fechaped: '$createdAt'

              }
            },
            {
              $lookup: {
                from: "categoria_productos",
                localField: "categoria",
                foreignField: "_id",
                as: "data_cat",
              },
            },
            {
              $group: {
                _id: "$categoria",
                categoria: { $last: "$data_cat.nombre" },
                puntosFt_asignados: { $sum: "$puntosUnidad" },
                puntosFt_ganados: { $sum: "$puntosFt_ganados" },
              },
            },

          ],
        },
      },


    ]).exec(callback);
  },
  /** Trae toda la informacion del componente de puntos-feat de una organizacion */
  getPuntosFtPorProducto: function (query, callback) {
    var ObjectId = require("mongoose").Types.ObjectId;
    this.aggregate([
      {
        $match: {
          idOrganizacion: new ObjectId(query),
          puntos_ft_ganados: {
            $gt: 0,
          }
        },

      },
      {
        $lookup: {
          from: "productos",
          localField: "productoId",
          foreignField: "_id",
          as: "data_prod",
        },
      },
      {
        $project: {
          idProducto: '$productoId',
          producto: { $arrayElemAt: ['$data_prod.nombre', 0] },
          puntosFt_asignados: '$puntos_ft_unidad',
          puntosFt_ganados: { $sum: '$puntos_ft_ganados' },
          codigo_organizacion: '$codigo_organizacion_producto',
          puntosFt_fecha_inicio: { $arrayElemAt: ['$data_prod.fecha_apertura_puntosft', 0] },
          puntosFt_fecha_cierre: { $arrayElemAt: ['$data_prod.fecha_cierre_puntosft', 0] },
          marca: '$marcaProducto',
          categoria: '$categoriaProducto',
          fechaped: '$createdAt'

        }
      },
      {
        $group: {
          _id: "$producto",
          idProducto: { $last: '$idProducto' },
          producto: { $last: '$producto' },
          puntosFt_asignados: { $last: '$puntosFt_asignados' },
          puntosFt_ganados: { $sum: '$puntosFt_ganados' },
          codigo_organizacion: { $last: '$codigo_organizacion' },
          puntosFt_fecha_inicio: { $last: '$puntosFt_fecha_inicio' },
          puntosFt_fecha_cierre: { $last: '$puntosFt_fecha_cierre' },
          marca: { $last: '$marca' },
          categoria: { $last: '$categoria' },
          fechaped: { $last: '$fechaped' },
        },
      },
    ]).exec(callback);
  },

  /** Distribuidores de una organizacion */
  informePuntosFTDistribuidor: function (query, callback) {
    let ObjectId = require("mongoose").Types.ObjectId;
    const page = query.pagina;
    const skip = (page - 1) * 10;
    this.aggregate([
      /** Arma la data de la grafica */
      {
        $match: {
          idDistribuidor: new ObjectId(query.idDistribuidor),
          descuento: { $gt: 0 }
        },
      },
      {
        $skip: skip
      },
      {
        $limit: 10
      },
      {
        $group: {
          _id: "$idPedido",
          puntos_ft_ganados: { $first: "$puntosGanados" },
          idUserHoreca: { $first: "$idUserHoreca" },
          puntoEntrega: { $first: "$idPunto" },
          pedido: { $first: "$idPedido" },
          fecha: {
            $first: {
              $dateToString: {
                format: "%Y-%m-%d",
                date: "$createdAt",
              },
            },
          },
        },
      },
      {
        $lookup: {
          from: "usuario_horecas",
          localField: "idUserHoreca", //Nombre del campo de la coleccion actual
          foreignField: "_id", //Nombre del campo de la coleccion a relacionar
          as: "datahoreca", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
      {
        $lookup: {
          from: "punto_entregas",
          localField: "puntoEntrega", //Nombre del campo de la coleccion actual
          foreignField: "_id", //Nombre del campo de la coleccion a relacionar
          as: "dataPuntoEntrega", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
      {
        $lookup: {
          from: "pedidos",
          localField: "pedido", //Nombre del campo de la coleccion actual
          foreignField: "_id", //Nombre del campo de la coleccion a relacionar
          as: "dataPedido", //Nombre del campo donde se insertara todos los documentos relacionados
          pipeline: [
            {
              $lookup: {
                from: "codigos_descuento_generados",
                localField: "codigo_descuento", //Nombre del campo de la coleccion actual
                foreignField: "_id", //Nombre del campo de la coleccion a relacionar
                as: "dataCodigo", //Nombre del campo donde se insertara todos los documentos relacionados
              },
            },
          ],
        },
      },
      {
        $sort: { fecha: -1 },
      },
      {
        $project: {
          puntos_ganados: "$puntos_ft_ganados",
          fecha: "$fecha",
          idPedido: "$dataPedido.id_pedido",
          list_code: "$dataPedido.dataCodigo",
          codigoRedencion: { $first: "$dataPedido.dataCodigo.codigo_creado" },
          puntoEntrega: { $first: "$dataPuntoEntrega.nombre" },
          establecimiento: { $first: "$datahoreca.nombre_establecimiento" },
          nit_establecimiento: { $first: "$datahoreca.nit" },
          tipo_persona_establecimiento: { $first: "$datahoreca.tipo_usuario" },
          tipo_negocio_establecimiento: { $first: "$datahoreca.tipo_negocio" },
          valor_pedido: "$dataPedido.total_pedido"
        },
      },
    ]).exec(callback);
  },

  informePuntosPorMesDistribuidorNew: function (query, callback) {
    let inicio = query.anio + '-01-01';
    let fin = query.anio + '-12-31';
    let ObjectId = require("mongoose").Types.ObjectId;
    this.aggregate([
      {
        $match: {
          idDistribuidor: new ObjectId(query.idDistribuidor),
          descuento: { $gt: 0 }
        },
      },
      {
        $facet: {
          puntosFeatMes: [
            {
              $group: {
                _id: "$idPedido",
                puntos_ft_ganados: { $first: "$puntosGanados" },
                idUserHoreca: { $first: "$idUserHoreca" },
                puntoEntrega: { $first: "$idPunto" },
                pedido: { $first: "$idPedido" },
                fecha: {
                  $first: "$createdAt",
                },
              },
            },
            {
              $lookup: {
                from: "pedidos",
                localField: "pedido",
                foreignField: "_id",
                as: "dataPedido",
                pipeline: [
                  {
                    $lookup: {
                      from: "codigos_descuento_generados",
                      localField: "codigo_descuento",
                      foreignField: "_id",
                      as: "dataCodigo",
                    },
                  },
                  {
                    $unwind: '$dataCodigo'
                  }
                ],
              },
            },
            {
              $unwind: '$dataPedido'
            },
            {
              $replaceRoot: {
                newRoot: "$dataPedido",
              },
            },
            {
              $match: {
                createdAt: {
                  $gte: new Date(inicio),
                  $lte: new Date(fin),
                }
              }
            },
            {
              $addFields: {
                puntosFt_fecha: {
                  $dateToString: {
                    format: "%Y-%m",
                    date: "$createdAt",
                  },
                },
              },
            },
            {
              $group: {
                _id: "$puntosFt_fecha",
                total: { $sum: "$dataCodigo.valor_paquete" },
              },
            },
            {
              $sort: { _id: 1 },
            }

          ],
          establecimientoGrafica: [
            {
              $group: {
                _id: "$idPedido",
                puntos_ft_ganados: { $first: "$puntosGanados" },
                idUserHoreca: { $first: "$idUserHoreca" },
                puntoEntrega: { $first: "$idPunto" },
                pedido: { $first: "$idPedido" }
              },
            },
            {
              $lookup: {
                from: "pedidos",
                localField: "pedido",
                foreignField: "_id",
                as: "dataPedido",
                pipeline: [
                  {
                    $lookup: {
                      from: "codigos_descuento_generados",
                      localField: "codigo_descuento",
                      foreignField: "_id",
                      as: "dataCodigo",
                    },
                  },
                  {
                    $unwind: '$dataCodigo'
                  }
                ],
              },
            },
            {
              $unwind: '$dataPedido'
            },
            {
              $replaceRoot: {
                newRoot: "$dataPedido",
              },
            },
            {
              $match: {
                createdAt: {
                  $gte: new Date(inicio),
                  $lte: new Date(fin),
                }
              }
            },
            {
              $addFields: {
                puntosFt_fecha: {
                  $dateToString: {
                    format: "%Y-%m",
                    date: "$createdAt",
                  },
                },
              },
            },
            {
              $group: {
                _id: {
                  puntosFt_fecha: "$puntosFt_fecha",
                  punto_entrega: "$punto_entrega",
                },
                puntosFt_fecha: { $first: "$puntosFt_fecha" },
                total: { $sum: 1 },
              },
            },
            {
              $group: {
                _id: "$puntosFt_fecha",
                total: { $sum: 1 },
              },
            },
            {
              $sort: { _id: 1 },
            },
          ],
          codigosRedencion: [
            {
              $group: {
                _id: "$idPedido",
                puntos_ft_ganados: { $first: "$puntosGanados" },
                idUserHoreca: { $first: "$idUserHoreca" },
                puntoEntrega: { $first: "$idPunto" },
                pedido: { $first: "$idPedido" }
              },
            },
            {
              $lookup: {
                from: "pedidos",
                localField: "pedido",
                foreignField: "_id",
                as: "dataPedido",
                pipeline: [
                  {
                    $lookup: {
                      from: "codigos_descuento_generados",
                      localField: "codigo_descuento",
                      foreignField: "_id",
                      as: "dataCodigo",
                    },
                  },
                  {
                    $unwind: '$dataCodigo'
                  }
                ],
              },
            },
            {
              $unwind: '$dataPedido'
            },
            {
              $replaceRoot: {
                newRoot: "$dataPedido",
              },
            },
            {
              $match: {
                createdAt: {
                  $gte: new Date(inicio),
                  $lte: new Date(fin),
                }
              }
            },
            {
              $addFields: {
                puntosFt_fecha: {
                  $dateToString: {
                    format: "%Y-%m",
                    date: "$createdAt",
                  },
                },
              },
            },
            {
              $group: {
                _id: {
                  codigo_creado: "$dataCodigo.codigo_creado",
                  punto_entrega: "$punto_entrega",
                },
                puntosFt_fecha: { $first: "$puntosFt_fecha" },
                total: { $sum: 1 },
              },
            },
            {
              $group: {
                _id: "$puntosFt_fecha",
                total: { $sum: 1 },
              },
            },
            {
              $sort: { _id: 1 },
            }
          ],
        }
      }

    ]).exec(callback);
  },
  /** TABLA Ventas distribuidor */
  getInformeDistribuidorTablaVentas2: function (query, callback) {
    let ObjectId = require("mongoose").Types.ObjectId;
    this.aggregate([
      {
        $match: {
          idDistribuidor: query.query,
        },
      },

      {
        $match: {
          $or: [
            { estaActTraking: "Entregado" },
            { estaActTraking: "Recibido" },
            { estaActTraking: "Calificado" },
          ],
        },
      },
      {
        $sort: {
          createdAt: -1
        }
      },
      {
        $skip: query.skip
      },
      {
        $limit: 10
      },
      {
        $lookup: {
          from: "punto_entregas",
          localField: "idPunto", //Nombre del campo de la coleccion actual
          foreignField: "_id", //Nombre del campo de la coleccion a relacionar
          pipeline: [
            {
              $project: {
                nombre: "$nombre",
                ciudad: "$ciudad",
                sillas: "$sillas",
                domicilios: "$domicilios",
                departamento: "$departamento",
                pais: "$pais",
                usuario_horeca: "$usuario_horeca",
              },
            },
          ],
          as: "data_punto", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
      {
        $lookup: {
          from: "usuario_horecas", //Nombre de la colecccion a relacionar
          localField: "data_punto.usuario_horeca", //Nombre del campo de la coleccion actual
          foreignField: "_id", //Nombre del campo de la coleccion a relacionar
          pipeline: [
            {
              $project: {
                tipo_usuario: "$tipo_usuario",
                nit: "$nit",
                nombre_establecimiento: "$nombre_establecimiento",
                tipo_negocio: "$tipo_negocio",
              },
            },
          ],
          as: "data_horeca", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
      {
        $lookup: {
          from: "pedidos",
          localField: "idPedido",
          foreignField: "_id",
          pipeline: [
            {
              $project: {
                id_pedido: "$id_pedido",
              },
            },
          ],
          as: "data_pedido",
        },
      },
      {
        $lookup: {
          from: "distribuidores_vinculados",
          let: {
            punto: "$idPunto",
            distribuidor: "$idDistribuidor",
          },
          /** Retorna solo las compras entre punto y distribuidor */
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
                from: "trabajadors", //Nombre de la colecccion a relacionar
                localField: "vendedor", //Nombre del campo de la coleccion actual
                foreignField: "_id", //Nombre del campo de la coleccion a relacionar
                pipeline: [
                  {
                    $project: {
                      nombre: { $concat: ["$nombres", " ", "$apellidos"] },
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
          from: "categoria_productos",
          localField: "categoriaProducto",
          foreignField: "_id",
          pipeline: [
            {
              $project: {
                nombre: "$nombre",
              },
            },
          ],
          as: "data_categoria",
        },
      },
      {
        $lookup: {
          from: "marca_productos",
          localField: "marcaProducto",
          foreignField: "_id",
          pipeline: [
            {
              $project: {
                nombre: "$nombre",
              },
            },
          ],
          as: "data_marca",
        },
      },
      {
        $lookup: {
          from: "organizacions", //Nombre de la colecccion a relacionar
          localField: "idOrganizacion", //Nombre del campo de la coleccion actual
          foreignField: "_id", //Nombre del campo de la coleccion a relacionar
          pipeline: [
            {
              $project: {
                nombre: "$nombre",
              },
            },
          ],
          as: "data_organizacion", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
      {
        $lookup: {
          from: "productos",
          localField: "productoId",
          foreignField: "_id",
          as: "producto_data_actual",
        },
      },
      {
        $addFields: {
          linea_producto: {
            $arrayElemAt: ["$producto_data_actual.linea_producto", 0],
          },
          producto_costo_total: {
            $multiply: ["$unidadesCompradas", "$costoProductos"],
          },
          producto_puntos_ganados: {
            $multiply: [
              "$unidadesCompradas",
              {
                $arrayElemAt: ["$detalleProducto.precios.puntos_ft_unidad", 0],
              },
            ],
          },
          producto_precios: {
            $arrayElemAt: ["$detalleProducto.precios", 0],
          },
        },
      },
      {
        $lookup: {
          from: "linea_productos",
          localField: "linea_producto",
          foreignField: "_id",
          pipeline: [
            {
              $project: {
                nombre: "$nombre",
              },
            },
          ],
          as: "data_linea",
        },
      },
      {
        $project: {
          _id: "$_id",
          pedido_fecha: "$createdAt",
          pedido_id: {
            $arrayElemAt: ["$data_pedido.id_pedido", 0],
          },
          horeca_nombre: {
            $arrayElemAt: ["$data_horeca.nombre_establecimiento", 0],
          },
          horeca_tipo_usuario: {
            $arrayElemAt: ["$data_horeca.tipo_usuario", 0],
          },
          horeca_nit: { $arrayElemAt: ["$data_horeca.nit", 0] },
          horeca_tipo_negocio: {
            $arrayElemAt: ["$data_horeca.tipo_negocio", 0],
          },
          punto_pais: { $arrayElemAt: ["$data_punto.pais", 0] },
          punto_departamento: {
            $arrayElemAt: ["$data_punto.departamento", 0],
          },
          punto_ciudad: { $arrayElemAt: ["$data_punto.ciudad", 0] },
          punto_nombre: { $arrayElemAt: ["$data_punto.nombre", 0] },
          punto_sillas: { $arrayElemAt: ["$data_punto.sillas", 0] },
          punto_domicilios: { $arrayElemAt: ["$data_punto.domicilios", 0] },
          producto_nombre: "$nombreProducto",
          producto_categoria: {
            $arrayElemAt: ["$data_categoria.nombre", 0],
          },
          producto_linea: {
            $arrayElemAt: ["$data_linea.nombre", 0],
          },
          producto_marca: {
            $arrayElemAt: ["$data_marca.nombre", 0],
          },
          producto_organizacion: {
            $arrayElemAt: ["$data_organizacion.nombre", 0],
          },
          producto_codigo_organizacion: "$codigo_organizacion_producto",
          producto_codigo_distribuidor: "$codigoDistribuidorProducto",
          producto_descripcion: "$detalleProducto.descripcion",
          producto_unidad_medida: "$producto_precios.unidad_medida",
          producto_cantidad_medida: "$producto_precios.cantidad_medida",
          pedido_cajas: "$caja",
          pedido_unidades: "$unidadesCompradas",
          pedido_total: "$producto_costo_total",
          pedido_puntos_acumulados: "$producto_puntos_ganados",
          equipo_comercial: {
            $arrayElemAt: [
              {
                $arrayElemAt: ["$data_vinculacion.data_trabajador.nombre", 0],
              },
              0,
            ],
          },
        },
      },

    ]).exec(callback);
  },
};

const ReportePed = (module.exports = mongoose.model(
  "ReportePedidos",
  ReportePedSchema
));
