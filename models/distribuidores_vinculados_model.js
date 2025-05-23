const mongoose = require("mongoose");
// Schema
const PuntoEntrega = require('./punto_entrega_model'); // Ajusta la ruta según tu proyecto
const DistribuidorBD = require("./distribuidor_model");
const Puntos_entregaBD = "punto_entregas";
const productosDistribuidor = require("./productos_por_distribuidor_model");
const productos = require("./producto_model");
const Usiario_horecaDB = require("./usuario_horeca_model");
const marca = require("./marca_producto_model");
const ObjectId = require("mongoose").Types.ObjectId;
const moment = require("moment");
const marcaBD = require("./marca_producto_model");
const lineaBD = require("./linea_producto_model");
const TrabajadorDist = require("./trabajador_model");

const Distribuidores_vinculadosSchema = mongoose.Schema({
  estado: {
    type: String,
    required: true,
    default: "Aprobado",
    enum: ["Pendiente", "Aprobado", "Rechazado", "Cancelado"],
  },
  convenio: {
    type: Boolean,
    required: true,
  },
  cartera: {
    type: Boolean,
    required: true,
  },
  punto_entrega: {
    type: mongoose.Schema.Types.ObjectId,
    ref: "Punto_entrega",
    required: true,
  },
  distribuidor: {
    type: mongoose.Schema.Types.ObjectId,
    ref: "Distribuidor",
    required: true,
  },
  usuario_registrado: {
    type: Boolean,
    default: false
  },
  vendedor: {
    type: [mongoose.Schema.Types.ObjectId],
    ref: "Trabajador",
    required: true,
  },
  pazysalvo: {
    type: Boolean,
    required: true,
  },
  slug_fact: {
    type: String,
    required: false,
  },
  creado_distribuidor: {
    type: Boolean,
    required: false,
    default: false
  },
  createdAt: {
    type: Date,
    required: false,
    default: Date.now,
  },
});

Distribuidores_vinculadosSchema.statics = {
  get: function (query, callback) {
    this.findOne(query)
      .populate({
        path: "punto_entrega",
        populate: {
          path: "usuario_horeca"
        },
      })
      .exec(callback);
  },
  getAllVinculations: function (query, callback) {
    this.find(query)
      .populate({
        path: "punto_entrega",
        populate: {
          path: "usuario_horeca"
        },
      })
      .exec(callback);
  },
  getAllIdVend: function (query, callback) {
    this.find(query)
      .exec(callback);
  },
  getAll: function (query, callback) {
    this.find(query).populate("distribuidor", ["nombre"]).exec(callback);
  },
  getInfoDataHoreca: function (query, callback) {
    this.aggregate([
      {
        $match: {
          distribuidor: new ObjectId(query),
          estado: "Aprobado",
        },
      },
      {
        $lookup: {
          from: Puntos_entregaBD, //Nombre de la colecccion a relacionar
          localField: "punto_entrega", //Nombre del campo de la coleccion actual
          foreignField: "_id", //Nombre del campo de la coleccion a relacionar
          as: "data_punto", //Nombre del campo donde se insertara todos los documentos relacionados
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
        $lookup: {
          from: 'trabajadors', //Nombre de la colecccion a relacionar
          localField: "usuario_horeca", //Nombre del campo de la coleccion actual
          foreignField: "usuario_horeca", //Nombre del campo de la coleccion a relacionar
          as: "dataUser", //Nombre del campo donde se insertara todos los documentos relacionados
          pipeline: [
            {
              $match: {
                $or: [
                  { puntos_entrega: '$_id' },
                  { tipo_trabajador: "ADMINISTRADOR APROBADOR" },
                  { tipo_trabajador: "PROPIETARIO/REP LEGAL" },
                ],
              },
            }
          ]
        },
      },
      {
        $unwind: "$dataUser",
      },
      {
        $replaceRoot: {
          newRoot: "$dataUser",
        },
      },
    ]).exec(callback);
  },
  distribuidores_vinculados_by_trabajador: function (query, callback) {
    this.aggregate([
      {
        $match: {
          vendedor: new ObjectId(query),
        },
      },
      {
        $lookup: {
          from: Puntos_entregaBD, //Nombre de la colecccion a relacionar
          localField: "punto_entrega", //Nombre del campo de la coleccion actual
          foreignField: "_id", //Nombre del campo de la coleccion a relacionar
          as: "data_punto", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
      {
        $project: {
          vendedor: '$vendedor',
          id_modificar: '$_id',
          nombre_punto: '$data_punto.nombre',
          id_punto: '$data_punto._id'
        }
      }
    ]).exec(callback);
  },
  updateById: function (id, updateData, callback) {
    this.findOneAndUpdate(
      { _id: id },
      { $set: updateData },
      { new: true },
      callback
    );
  },
  setVendedores: function (id, vendedores, callback) {
    this.findOneAndUpdate(
      { _id: id },
      {
        $set: {
          vendedor: vendedores,
        },
      },
      callback
    );
  },
  getPuntosByDistribuidor: function (query, callback) {
    this.aggregate([
      {
        $match: {
          distribuidor: new ObjectId(query),
          estado: "Aprobado",
        },
      },
      {
        $lookup: {
          from: Puntos_entregaBD, //Nombre de la colecccion a relacionar
          localField: "punto_entrega", //Nombre del campo de la coleccion actual
          foreignField: "_id", //Nombre del campo de la coleccion a relacionar
          as: "data_punto", //Nombre del campo donde se insertara todos los documentos relacionados
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
    ]).exec(callback);
  },
  getAllPuntosDistribuidor: function (query, callback) {
    this.aggregate([
      {
        $match: {
          distribuidor: new ObjectId(query.distribuidor),
        },
      },
    ]).exec(callback);
  },
  removeById: function (removeData, callback) {
    this.findOneAndRemove(removeData, callback);
  },
  removeAll: function (query, callback) {
    this.remove(query).exec(callback);
  },
  create: function (data, callback) {
    const Distribuidores_vinculados_data = new this(data);
    Distribuidores_vinculados_data.save(callback);
  },
  getPuntosVinculadosDistribuidor: function (obj, callback) {
    const query = {
      distribuidor: new ObjectId(obj.distribuidor),
      estado: "Aprobado",
    };
    this.find(query).exec(callback);
  },

  /**
   * @param {*} query
   * @param {*} callback
   */
  getVincionesHD: function (query, callback) {
    this.aggregate([
      {
        $lookup: {
          from: 'punto_entregas', //Nombre de la colecccion a relacionar
          localField: "punto_entrega", //Nombre del campo de la coleccion actual
          foreignField: "_id", //Nombre del campo de la coleccion a relacionar
          as: "data_punto", //Nombre del campo donde se insertara todos los documentos relacionados
          pipeline: [
            {
              $lookup: {
                from: 'usuario_horecas', //Nombre de la colecccion a relacionar
                localField: "usuario_horeca", //Nombre del campo de la coleccion actual
                foreignField: "_id", //Nombre del campo de la coleccion a relacionar
                as: "dataHoreca", //Nombre del campo donde se insertara todos los documentos relacionados
              },
            },

          ],
        },
      },
      {
        $project: {
          estadoVinculacion: "$estado",
          fechaVinculacion: { $dateToString: { format: "%Y/%m/%d", date: "$createdAt" } },
          punto: "$data_punto.nombre",
          ciudad_punto: "$data_punto.ciudad",
          telefono_punto: "$data_punto.telefono",
          direccion_punto: "$data_punto.direccion",
          departamento_punto: "$data_punto.departamento",
          estado_punto: "$data_punto.estado",
          encargado_punto: {
            $arrayElemAt: ["$data_punto.informacion_contacto", 0],
          },
          dataHoreca: "$data_punto.dataHoreca.nombre_establecimiento",
          dataHoreca2: "$data_punto.dataHoreca.razon_social",
          nombre_dist: "$data_distribuidor.nombre",
          correo_dist: "$data_distribuidor.correo",
          departamento_dist: "$data_distribuidor.departamento",
          departamento_dist: "$data_distribuidor.direccion",
          nit_cc_dist: "$data_distribuidor.nit_cc",
          tipo_dist: "$data_distribuidor.tipo_persona",
          tipo_negocio_dist: "$data_distribuidor.tipo",
          telefono_dist: "$data_distribuidor.celular",
          celular_dist: "$data_distribuidor.telefono",
          razon_dist: "$data_distribuidor.razon_social",
        },
      }
    ]).exec(callback);
  },
  getEstablecimientos_por_distribuidor: function (query, callback) {
    this.aggregate([
      {
        $lookup: {
          from: 'distribuidors', //Nombre de la colecccion a relacionar
          localField: "distribuidor", //Nombre del campo de la coleccion actual
          foreignField: "_id", //Nombre del campo de la coleccion a relacionar
          as: "data_distribuidor", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
      {
        $group: {
          _id: "$data_distribuidor.nombre",
          count: { $sum: 1 },
        },
      },
    ]).exec(callback);
  },
  getEstablecimientos_con_convenio: function (query, callback) {
    this.aggregate([
      {
        $match: {},
      },
      {
        $group: {
          _id: "$convenio",
          cantidad: { $sum: 1 },
        },
      },
    ]).exec(callback);
  },
  getEstablecimientos_quedados_cartera: function (query, callback) {
    this.aggregate([
      {
        $match: {},
      },
      {
        $group: {
          _id: "$pazysalvo",
          cantidad: { $sum: 1 },
        },
      },
    ]).exec(callback);
  },
  marcasSillasAlcanzadasMarcasMes: function (query, callback) {
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
          from: Puntos_entregaBD, //Nombre de la colecccion a relacionar
          localField: "punto_entrega", //Nombre del campo de la coleccion actual
          foreignField: "_id", //Nombre del campo de la coleccion a relacionar
          as: "Puntos", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
      {
        $lookup: {
          from: productosDistribuidor.collection.name, //Nombre de la colecccion a relacionar
          localField: "distribuidor", //Nombre del campo de la coleccion actual
          foreignField: "distribuidor", //Nombre del campo de la coleccion a relacionar
          pipeline: [
            {
              $lookup: {
                from: productos.collection.name, //Nombre de la colecccion a relacionar
                localField: "productos", //Nombre del campo de la coleccion actual
                foreignField: "_id", //Nombre del campo de la coleccion a relacionar
                pipeline: [
                  {
                    $lookup: {
                      from: marca.collection.name, //Nombre de la colecccion a relacionar
                      localField: "marca_producto", //Nombre del campo de la coleccion actual
                      foreignField: "_id", //Nombre del campo de la coleccion a relacionar
                      as: "infoMarcaProducto", //Nombre del campo donde se insertara todos los documentos relacionados
                    },
                  },
                  {
                    $unwind: "$infoMarcaProducto",
                  },
                  {
                    $replaceRoot: {
                      newRoot: "$infoMarcaProducto",
                    },
                  },
                ],
                as: "dataProductos", //Nombre del campo donde se insertara todos los documentos relacionados
              },
            },
            {
              $unwind: "$dataProductos",
            },
            {
              $replaceRoot: {
                newRoot: "$dataProductos",
              },
            },
          ],
          as: "productosPorDistribuidor", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
      {
        $addFields: {
          "productosPorDistribuidor.cant": { $sum: "$Puntos.sillas" },
        },
      },
      {
        $addFields: {
          "productosPorDistribuidor.idPunto": "$Puntos._id",
        },
      },
      {
        $project: {
          marcas: "$productosPorDistribuidor",
        },
      },
      {
        $unwind: "$marcas",
      },
      {
        $replaceRoot: {
          newRoot: "$marcas",
        },
      },
    ]).exec(callback);
  },
  /** Cantidad de distribuidores segun estado*/
  getInformePieDistribuidores: function (query, callback) {
    this.aggregate([
      {
        $match: {
          punto_entrega: new ObjectId(query.idPunto),
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
  /** Cantidad de distribuidores segun estado*/
  getAllVinculados_new: function (query, callback) {
    let fechaFin = moment().format();
    let fechaInicio = moment().subtract(3, 'M').format();
    this.aggregate([
      {
        $match: {
          distribuidor: new ObjectId(query),
          estado: 'Aprobado'
        },
      },
      {
        $lookup: {
          from: Puntos_entregaBD, //Nombre de la colecccion a relacionar
          localField: "punto_entrega", //Nombre del campo de la coleccion actual
          foreignField: "_id", //Nombre del campo de la coleccion a relacionar
          as: "punto_entrega", //Nombre del campo donde se insertara todos los documentos relacionados
          pipeline: [
            {
              $lookup: {
                from: 'usuario_horecas', //Nombre de la colecccion a relacionar
                localField: "usuario_horeca", //Nombre del campo de la coleccion actual
                foreignField: "_id", //Nombre del campo de la coleccion a relacionar
                as: "horeca", //Nombre del campo donde se insertara todos los documentos relacionados
              },
            },
            {
              $lookup: {
                from: 'reportepedidos', //Nombre de la colecccion a relacionar
                localField: "usuario_horeca", //Nombre del campo de la coleccion actual
                foreignField: "idPunto", //Nombre del campo de la coleccion a relacionar
                as: "pedidos", //Nombre del campo donde se insertara todos los documentos relacionados
                pipeline: [
                  {
                    $match: {

                      createdAt: {
                        $gte: new Date(fechaInicio),
                        $lte: new Date(fechaFin),
                      },
                    },
                  },
                  {
                    $match:
                    {
                      $or: [
                        { "estaActTraking": "Entregado" },
                        { "estaActTraking": "Recibido" },
                        { "estaActTraking": "Calificado" },
                      ],
                    },
                  },
                ],
              },
            },
            {
              $lookup: {
                from: 'trabajadors', //Nombre de la colecccion a relacionar
                localField: "usuario_horeca", //Nombre del campo de la coleccion actual
                foreignField: "usuario_horeca", //Nombre del campo de la coleccion a relacionar
                as: "userAdmin", //Nombre del campo donde se insertara todos los documentos relacionados
                pipeline: [
                  {
                    $match: { tipo_trabajador: "Administrador" },
                  },
                ],
              },
            },
          ],
        },
      },
    ]).exec(callback);
  },
};

const Distribuidores_vinculados = (module.exports = mongoose.model(
  "Distribuidores_vinculados",
  Distribuidores_vinculadosSchema
));

module.exports.getDistribuidoresPuntoEntregaTipo = function (obj, callback) {
  Distribuidores_vinculados.find(query)
    .populate({
      path: "punto_entrega",
      populate: {
        path: "usuario_horeca",
        query: { tipo_negocio: { $in: [obj.tipo_negocio] } },
      },
    })
    .exec(callback);
};

module.exports.getDistribuidoresByPunto = function (obj, callback) {
  const query = {
    punto_entrega: new ObjectId(obj.punto_entrega),
    estado: "Aprobado",
  };
  Distribuidores_vinculados.count(query, callback);
};
/** Recupera el total de clientes pendientes por aprobacióin de un distribuidor */
module.exports.getTotalVinculacionesPendientesByDistribuidor = function (
  obj,
  callback
) {
  const query = {
    estado: "Pendiente",
    distribuidor: obj.distribuidor,
  };
  Distribuidores_vinculados.count(query).exec(callback);
};
module.exports.getDistribuidoVinculadoDetallado = function (obj, callback) {
  const query = {
    estado: "Pendiente",
    distribuidor: new ObjectId(obj.distribuidor),
  };
  Distribuidores_vinculados.find(query)
    .populate("distribuidor vendedor")
    .populate({
      path: "punto_entrega",
      populate: {
        path: "usuario_horeca",
      },
    })
    .exec(callback);
};
module.exports.getDistribuidoVinculadoLista = function (obj, callback) {
  const query = {
    distribuidor: new ObjectId(obj.distribuidor),
  };
  Distribuidores_vinculados.find(query).exec(callback);
};
module.exports.getDistribuidorTipoEstablecimientoNombre = function (
  obj,
  callback
) {
  this.aggregate([
    {
      $match: {
        estado: "Pendiente",
        distribuidor: new ObjectId(obj.trim()),
      },
    },
    {
      $lookup: {
        from: Puntos_entregaBD, //Nombre de la colecccion a relacionar
        localField: "punto_entrega", //Nombre del campo de la coleccion actual
        foreignField: "_id", //Nombre del campo de la coleccion a relacionar
        as: "punto_entrega", //Nombre del campo donde se insertara todos los documentos relacionados
      },
    },
    {
      $unwind: "$punto_entrega",
    },
    {
      $replaceRoot: {
        newRoot: "$punto_entrega",
      },
    },
    {
      $project: {
        usuario_horeca: "$usuario_horeca",
      },
    },
    {
      $lookup: {
        from: Usiario_horecaDB.collection.name, //Nombre de la colecccion a relacionar
        localField: "usuario_horeca", //Nombre del campo de la coleccion actual
        foreignField: "_id", //Nombre del campo de la coleccion a relacionar
        as: "horeca", //Nombre del campo donde se insertara todos los documentos relacionados
      },
    },
    {
      $unwind: "$horeca",
    },
    {
      $replaceRoot: {
        newRoot: "$horeca",
      },
    },
    {
      $project: {
        tipo_negocio: "$tipo_negocio",
      },
    },
  ]).exec(callback);
};

module.exports.getLengthVinculados = function (obj, callback) {
  const query = {
    estado: "Aprobado",
    distribuidor: new ObjectId(obj.distribuidor),
  };
  Distribuidores_vinculados.count(query, callback);
};

module.exports.getDistribuidoVinculadoDetalladoHoreca = function (
  obj,
  callback
) {
  const query = {
    distribuidor: new ObjectId(obj.distribuidor),
  };
  Distribuidores_vinculados.find(query)
    .populate({
      path: "punto_entrega",
      populate: {
        path: "usuario_horeca",
      },
    })
    .populate("distribuidor")
    .exec(callback);
};

module.exports.getSolicitudPendiente = function (obj, callback) {
  const query = {
    estado: "Pendiente",
    punto_entrega: new ObjectId(obj.punto_entrega),
  };
  Distribuidores_vinculados.count(query, callback);
};

module.exports.getListaDistribuidoresByPunto = function (obj, callback) {
  const query = {
    punto_entrega: new ObjectId(obj.punto_entrega),
    estado: "Aprobado",
  };
  Distribuidores_vinculados.find(query)
    .populate("distribuidor punto_entrega")
    .exec(callback);
};

module.exports.getDistribuidorClientesAprobados = function (obj, callback) {
  const query = {
    distribuidor: new ObjectId(obj.distribuidor),
    estado: "Aprobado",
  };
  Distribuidores_vinculados.find(query)
    .populate("punto_entrega")
    .exec(callback);
};

module.exports.getDistribuidorClientesAprobadosByVendedor = function (
  obj,
  callback
) {
  const query = {
    distribuidor: new ObjectId(obj.distribuidor),
    estado: "Aprobado",
    vendedor: new ObjectId(obj.vendedor),
  };
  Distribuidores_vinculados.find(query)
    .populate("punto_entrega")
    .exec(callback);
};

module.exports.getVinculacionByDistribuidorYPunto = function (obj, callback) {
  const query = {
    distribuidor: new ObjectId(obj.distribuidor),
    punto_entrega: new ObjectId(obj.punto_entrega),
  };
  Distribuidores_vinculados.find(query)
    .populate("distribuidor punto_entrega")
    .exec(callback);
};
module.exports.getVinculacionPorDistYEstablecimiento = function (
  obj,
  callback
) {
  const query = new ObjectId(obj.distribuidor);
  this.aggregate([
    {
      $match: {
        distribuidor: query,
      },
    },
    {
      $match: {
        estado: "Aprobado",
      },
    },
    {
      $lookup: {
        from: "punto_entregas", //Nombre de la colecccion a relacionar
        localField: "punto_entrega", //Nombre del campo de la coleccion actual
        foreignField: "_id", //Nombre del campo de la coleccion a relacionar
        as: "puntos", //Nombre del campo donde se insertara todos los documentos relacionados
        pipeline: [
          {
            $lookup: {
              from: Usiario_horecaDB.collection.name, //Nombre de la colecccion a relacionar
              localField: "usuario_horeca", //Nombre del campo de la coleccion actual
              foreignField: "_id", //Nombre del campo de la coleccion a relacionar
              as: "horeca", //Nombre del campo donde se insertara todos los documentos relacionados
            },
          },
        ],
      },
    },
  ]).exec(callback);
};
/** Productos por ciudad seleccionada */
module.exports.getMiniaturasSaldosPromociones = function (req, callback) {
  this.aggregate([
    {
      $match: {
        punto_entrega: new ObjectId(req._idPunto),
        estado: "Aprobado",
      },
    },
    {
      $lookup: {
        from: DistribuidorBD.collection.name, //Nombre de la colecccion a relacionar
        localField: "distribuidor", //Nombre del campo de la coleccion actual
        foreignField: "_id", //Nombre del campo de la coleccion a relacionar
        pipeline: [
          {
            $project: {
              nombre: "$nombre",
              tipo: "$tipo",
              ranking: "$ranking",
              logo: "$logo",
              _id: "$_id",
              tiempo_entrega: "$tiempo_entrega"
            },
          },
        ],
        as: "data_distribuidor", //Nombre del campo donde se insertara todos los documentos relacionados
      },
    },
    {
      $lookup: {
        from: "pedidos", //Nombre de la colecccion a relacionar
        localField: "distribuidor", //Nombre del campo de la coleccion actual
        foreignField: "distribuidor", //Nombre del campo de la coleccion a relacionar
        pipeline: [
          {
            $match: {
              punto_entrega: new ObjectId(req._idPunto),
            },
          },
        ],
        as: "data_pedidos", //Nombre del campo donde se insertara todos los documentos relacionados
      },
    },
    {
      $project: {
        pazysalvo: "$pazysalvo",
        convenio: "$convenio",
        data_distribuidor: "$data_distribuidor",
        total_pedidos: { $size: "$data_pedidos" },
      },
    },
  ]).exec(callback);
};
/** Chequeo de precios por punto de entrega */
module.exports.getPreciosPorPunto = function (obj, callback) {
  const query = new ObjectId(obj.punto_entrega);
  this.aggregate([
    {
      $match: {
        punto_entrega: query,
        estado: "Aprobado",
      },
    },
    {
      $project: {
        distribuidor: "$distribuidor",
      },
    },
    {
      $lookup: {
        from: "distribuidors", //Nombre de la colecccion a relacionar
        localField: "distribuidor", //Nombre del campo de la coleccion actual
        foreignField: "_id", //Nombre del campo de la coleccion a relacionar
        as: "data_distribuidor", //Nombre del campo donde se insertara todos los documentos relacionados
        pipeline: [
          {
            $match: {
              solicitud_vinculacion: "Aprobado",
            },
          },
          {
            $project: {
              nombre: "$nombre",
            },
          },
        ],
      },
    },
    {
      $unwind: "$data_distribuidor",
    },
    {
      $replaceRoot: {
        newRoot: "$data_distribuidor",
      },
    },
    {
      $lookup: {
        from: productosDistribuidor.collection.name, //Nombre de la colecccion a relacionar
        localField: "_id", //Nombre del campo de la coleccion actual
        foreignField: "distribuidor", //Nombre del campo de la coleccion a relacionar
        as: "data_productos", //Nombre del campo donde se insertara todos los documentos relacionados
        pipeline: [
          {
            $lookup: {
              from: "productos", //Nombre de la colecccion a relacionar
              localField: "productos", //Nombre del campo de la coleccion actual
              foreignField: "_id", //Nombre del campo de la coleccion a relacionar
              pipeline: [
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

              ],
              as: "data_producto", //Nombre del campo donde se insertara todos los documentos relacionados
            },
          },
          {
            $unwind: "$data_producto",
          },
          {
            $replaceRoot: {
              newRoot: "$data_producto",
            },
          },
          {
            $project: {
              nombre: "$nombre",
              descripcion: "$descripcion",
              codigo_ft: "$codigo_ft",
              fotos: "$fotos",
              codigo_distribuidor: "$codigo_distribuidor",
              codigo_distribuidor_producto: "$codigo_distribuidor_producto",
              codigo_organizacion: "$codigo_organizacion",
              codigo_organizacion_producto: "$codigo_organizacion_producto",
              precios: "$precios",
              linea: "$linea_productos_query",
              marca: "$marca_productos_query",
              categoria: "$categoria_productos_query",
              priceMin: "$priceMin",
              priceMax: "$priceMax",
            },
          },
        ],
      },
    },
  ]).exec(callback);
};
/** Chequeo de precios por punto de entrega y CÓDIGO FEAT */
module.exports.getPreciosPorPuntoCodFeat = function (obj, callback) {
  const query = new ObjectId(obj.punto_entrega);
  const query_cod_feat = obj.cod_feat;
  this.aggregate([
    {
      $match: {
        punto_entrega: query,
        estado: "Aprobado",
      },
    },
    {
      $project: {
        distribuidor: "$distribuidor",
        pazysalvo: "$pazysalvo",
        convenio: "$convenio",
        cartera: "$cartera"
      },
    },
    {
      $lookup: {
        from: "documentos_usuario_solicitantes",
        localField: "distribuidor",
        foreignField: "usuario_distribuidor",
        as: "data_documentos",
        pipeline: [
          {
            $match: {
              estado: 'Aprobado',
              documento: { $in: ["Certificacion Cuenta", "Creacion de cliente", "Aprobacion de credito"] }
            }
          },
          {
            $group: {
              _id: "$documento",
              locacion: { $last: "$locacion" }
            }
          },
          {
            $project: {
              tipo: "$_id",
              locacion: "$locacion"
            }
          },
          {
            $unset: '_id'
          }
        ]
      }
    },
    {
      $lookup: {
        from: "distribuidors", //Nombre de la colecccion a relacionar
        localField: "distribuidor", //Nombre del campo de la coleccion actual
        foreignField: "_id", //Nombre del campo de la coleccion a relacionar
        as: "data_distribuidor", //Nombre del campo donde se insertara todos los documentos relacionados
        pipeline: [
          {
            $project: {
              nombre: "$nombre"
            },
          },
        ],
      },
    },
    {
      $addFields: {
        'data_distribuidor.pazysalvo': "$pazysalvo",
        'data_distribuidor.convenio': "$convenio",
        'data_distribuidor.cartera': "$cartera",
        'data_distribuidor.cant_votos_raking': "$cant_votos_raking",
        'data_distribuidor.ranking_gen': "$ranking_gen",
        'data_distribuidor.data_documentos': "$data_documentos"
      }
    },
    {
      $unwind: "$data_distribuidor",
    },
    {
      $replaceRoot: {
        newRoot: "$data_distribuidor",
      },
    },
    {
      $lookup: {
        from: productosDistribuidor.collection.name, //Nombre de la colecccion a relacionar
        localField: "_id", //Nombre del campo de la coleccion actual
        foreignField: "distribuidor", //Nombre del campo de la coleccion a relacionar
        as: "data_productos", //Nombre del campo donde se insertara todos los documentos relacionados
        pipeline: [
          {
            $lookup: {
              from: "productos", //Nombre de la colecccion a relacionar
              localField: "productos", //Nombre del campo de la coleccion actual
              foreignField: "_id", //Nombre del campo de la coleccion a relacionar
              pipeline: [
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
              ],
              as: "data_producto", //Nombre del campo donde se insertara todos los documentos relacionados
            },
          },
          {
            $unwind: "$data_producto",
          },
          {
            $replaceRoot: {
              newRoot: "$data_producto",
            },
          },
          {
            $match: {
              codigo_ft: query_cod_feat,
            },
          },
          {
            $lookup: {
              from: "distribuidors", //Nombre de la colecccion a relacionar
              localField: "codigo_distribuidor", //Nombre del campo de la coleccion actual
              foreignField: "_id", //Nombre del campo de la coleccion a relacionar
              pipeline: [
                {
                  $addFields: {
                    _idDistribuidor: "$_id",
                    nombreDistribuidor: "$nombre",
                    logo: "$logo",
                  },
                },
              ],
              as: "data_distribuidor", //Nombre del campo donde se insertara todos los documentos relacionados
            },
          },
          {
            $project: {
              _idProducto: "$_idProducto",
              nombre: "$nombre",
              descripcion: "$descripcion",
              codigo_distribuidor: "$codigo_distribuidor",
              codigo_distribuidor_producto: "$codigo_distribuidor_producto",
              codigo_organizacion: "$codigo_organizacion",
              codigo_organizacion_producto: "$codigo_organizacion_producto",
              codigo_ft: "$codigo_ft",
              fotos: "$fotos",
              precios: "$precios",
              marca: "$marca_productos_query",
              linea: "$linea_productos_query",
              data_distribuidor: "$data_distribuidor",
            },
          },
        ],
      },
    },
  ]).exec(callback);
};
/** Ventas por usuario comercial */
module.exports.getInformeDistEquipComerVentas = function (query, callback) {
  this.aggregate([
    {
      $match: {
        // estado: "Aprobado",
        distribuidor: new ObjectId(query.idDistribuidor),
      },
    },
    /** Busca las transacciones entre punto y dist */
    {
      $lookup: {
        from: "reportepedidos",
        let: {
          first: "$punto_entrega",
          second: "$distribuidor",
        },
        pipeline: [
          /** Retorna solo las compras entre punto y distribuidor */
          {
            $match: {
              createdAt: {
                $gte: new Date(query.inicio),
                $lte: new Date(query.fin),
              },
              $expr: {
                $and: [
                  {
                    $eq: ["$idPunto", "$$first"],
                  },
                  {
                    $eq: ["$idDistribuidor", "$$second"],
                  },
                ],
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
            $group: {
              _id: "$idPunto",
              total: { $sum: "$costoTotalProducto" },
            },
          },
        ],
        as: "data_compras",
      },
    },
    /** Busca el nombre de los vendedores de cada punto-dist */
    {
      $lookup: {
        from: "trabajadors",
        localField: "vendedor", //Nombre del campo de la coleccion actual
        foreignField: "_id", //Nombre del campo de la coleccion a relacionar
        pipeline: [
          {
            $match: {
              solicitud_vinculacion: "Aprobado",
            },
          },
          {
            $project: {
              nombres: { $concat: ["$nombres", " ", "$apellidos"] },
            },
          },
        ],
        as: "data_trabajadores", //Nombre del campo donde se insertara todos los documentos relacionados
      },
    },
    /** Le agrega al trabajador el total de pedidos realizados */
    {
      $addFields: {
        "data_trabajadores.costoTotalProducto": {
          $arrayElemAt: ["$data_compras.total", 0],
        },
      },
    },
    {
      $unwind: "$data_trabajadores",
    },
    {
      $replaceRoot: {
        newRoot: "$data_trabajadores",
      },
    },
    {
      $group: {
        _id: "$nombres",
        total: { $sum: "$costoTotalProducto" },
      },
    },
    {
      $sort: { total: -1 },
    },
  ]).exec(callback);
};
/** Establecimientos por usuario comercial */
module.exports.getInformeDistEquipComerTotalEstab = function (query, callback) {
  this.aggregate([
    {
      $match: {
        // estado: "Aprobado",
        distribuidor: new ObjectId(query.idDistribuidor),
      },
    },
    /** Evita tener puntos repetidos por dist */
    {
      $group: {
        _id: "$punto_entrega",
        vendedor: { $first: "$vendedor" },
      },
    },
    /** Se recupera el nombre del trabajador */
    {
      $lookup: {
        from: "trabajadors",
        localField: "vendedor" /** Nombre del campo de la coleccion actual */,
        foreignField:
          "_id" /** Nombre del campo de la coleccion a relacionar */,
        pipeline: [
          {
            $project: {
              nombres: { $concat: ["$nombres", " ", "$apellidos"] },
            },
          },
        ],
        as: "data_trabajadores" /** Nombre del campo donde se insertara todos los documentos relacionados */,
      },
    },
    /** Separa el array de trabajadores en objetos apartes */
    {
      $unwind: "$data_trabajadores",
    },
    /** Agrupa los trabajadores por nombre */
    {
      $group: {
        _id: "$data_trabajadores.nombres",
        total: { $sum: 1 },
      },
    },
    {
      $sort: { total: -1 },
    },
  ]).exec(callback);
};
/** Numero de referencias promedio por usuario comercial */
module.exports.getInformeDistEquipComerProdXPed = function (query, callback) {
  this.aggregate([
    {
      $match: {
        // estado: "Aprobado",
        distribuidor: new ObjectId(query.idDistribuidor),
      },
    },
    {
      $unwind: "$vendedor",
    },
    /** Busca las transacciones entre punto y dist */
    {
      $lookup: {
        from: "pedidos",
        let: {
          first: "$punto_entrega",
          second: "$distribuidor",
        },
        pipeline: [
          /** Retorna solo las compras entre punto y distribuidor */
          {
            $match: {
              createdAt: {
                $gte: new Date(query.inicio),
                $lte: new Date(query.fin),
              },
              $expr: {
                $and: [
                  {
                    $eq: ["$punto_entrega", "$$first"],
                  },
                  {
                    $eq: ["$distribuidor", "$$second"],
                  },
                ],
              },
              $or: [
                { estado: "Entregado" },
                { estado: "Recibido" },
                { estado: "Calificado" },
              ],
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
      $lookup: {
        from: "trabajadors",
        localField: "vendedor", //Nombre del campo de la coleccion actual
        foreignField: "_id", //Nombre del campo de la coleccion a relacionar
        pipeline: [
          {
            $match: {
              solicitud_vinculacion: "Aprobado",
            },
          },
          {
            $project: {
              nombres: { $concat: ["$nombres", " ", "$apellidos"] },
            },
          },
        ],
        as: "data_trabajadores", //Nombre del campo donde se insertara todos los documentos relacionados
      },
    },
    {
      $unwind: "$data_trabajadores",
    },
    // Le agrega al trabajador el total de productos pedidos
    {
      $addFields: {
        "data_compras.vendedor": "$data_trabajadores.nombres",
        "data_compras.total_productos": {
          $sum: "$data_compras.productos.unidad",
        },
      },
    },
    {
      $unwind: "$data_compras",
    },
    {
      $replaceRoot: {
        newRoot: "$data_compras",
      },
    },
    {
      $group: {
        _id: "$vendedor",
        total_pedidos: { $sum: 1 },
        total_produtos: { $sum: "$total_productos" },
      },
    },
    {
      $project: {
        _id: "$_id",
        total: {
          $trunc: [
            {
              $divide: ["$total_produtos", "$total_pedidos"],
            },
            2,
          ],
        },
      },
    },
    {
      $sort: { total: -1 },
    },
  ]).exec(callback);
};
/** Numero de pedidos por usuario comercial */
module.exports.getInformeDistEquipComerTotalPed = function (query, callback) {
  this.aggregate([
    {
      $match: {
        // estado: "Aprobado",
        distribuidor: new ObjectId(query.idDistribuidor),
      },
    },
    /** Busca las transacciones entre punto y dist */
    {
      $lookup: {
        from: "reportepedidos",
        let: {
          first: "$punto_entrega",
          second: "$distribuidor",
        },
        pipeline: [
          /** Retorna solo las compras entre punto y distribuidor */
          {
            $match: {
              createdAt: {
                $gte: new Date(query.inicio),
                $lte: new Date(query.fin),
              },
              $expr: {
                $and: [
                  {
                    $eq: ["$idPunto", "$$first"],
                  },
                  {
                    $eq: ["$idDistribuidor", "$$second"],
                  },
                ],
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
              _id: "$idPedido",
              total: { $sum: 1 },
            },
          },
        ],
        as: "data_compras",
      },
    },
    /** Busca el nombre de los vendedores de cada punto-dist */
    {
      $lookup: {
        from: "trabajadors",
        localField: "vendedor", //Nombre del campo de la coleccion actual
        foreignField: "_id", //Nombre del campo de la coleccion a relacionar
        pipeline: [
          {
            $match: {
              solicitud_vinculacion: "Aprobado",
            },
          },
          {
            $project: {
              nombres: { $concat: ["$nombres", " ", "$apellidos"] },
            },
          },
        ],
        as: "data_trabajadores", //Nombre del campo donde se insertara todos los documentos relacionados
      },
    },
    /** Le agrega al trabajador el total de productos pedidos */
    {
      $addFields: {
        "data_trabajadores.total_pedidos": {
          $arrayElemAt: ["$data_compras.total", 0],
        },
      },
    },
    /** Crea un objeto para cada trabajador */
    {
      $unwind: "$data_trabajadores",
    },
    {
      $group: {
        _id: "$data_trabajadores.nombres",
        total: { $sum: "$data_trabajadores.total_pedidos" },
      },
    },
    {
      $match: {
        total: { $gt: 0 },
      },
    },
    {
      $sort: { total: -1 },
    },
  ]).exec(callback);
};
/** Numero de sillas alcanzadas por usuario comercial */
module.exports.getInformeDistEquipComerSillas = function (query, callback) {
  this.aggregate([
    {
      $match: {
        estado: "Aprobado",
        distribuidor: new ObjectId(query.idDistribuidor),
      },
    },
    /** Busca el nombre de los vendedores de cada punto-dist */
    {
      $lookup: {
        from: "trabajadors",
        localField: "vendedor", //Nombre del campo de la coleccion actual
        foreignField: "_id", //Nombre del campo de la coleccion a relacionar
        pipeline: [
          {
            $match: {
              solicitud_vinculacion: "Aprobado",
            },
          },
          {
            $project: {
              nombre_trabajador: { $concat: ["$nombres", " ", "$apellidos"] },
            },
          },
        ],
        as: "data_trabajadores", //Nombre del campo donde se insertara todos los documentos relacionados
      },
    },
    /** Busca el nombre de los vendedores de cada punto-dist */
    {
      $lookup: {
        from: "punto_entregas",
        localField: "punto_entrega", //Nombre del campo de la coleccion actual
        foreignField: "_id", //Nombre del campo de la coleccion a relacionar
        pipeline: [
          {
            $project: {
              sillas: "$sillas",
            },
          },
        ],
        as: "data_puntos", //Nombre del campo donde se insertara todos los documentos relacionados
      },
    },
    /** Le agrega al trabajador el total de productos pedidos */
    {
      $addFields: {
        "data_trabajadores.total_sillas": {
          $arrayElemAt: ["$data_puntos.sillas", 0],
        },
      },
    },
    /** Crea un objeto para cada trabajador */
    {
      $unwind: "$data_trabajadores",
    },
    {
      $group: {
        _id: "$data_trabajadores.nombre_trabajador",
        total: {
          $sum: "$data_trabajadores.total_sillas",
        },
      },
    },
    {
      $sort: { total: -1 },
    },
  ]).exec(callback);
};
/** Distribuidor Tabla Informes Clientes */
module.exports.getInformeDistribuidorTablaClientes = function (
  query,
  callback
) {
  const d = new Date();
  const fecha_referencia = d.setMonth(d.getMonth() - 3);
  this.aggregate([
    {
      $match: {
        distribuidor: new ObjectId(query.idDistribuidor),
      },
    },
    /** Busca las transacciones entre punto y dist */
    {
      $lookup: {
        from: "reportepedidos",
        let: {
          first: "$punto_entrega",
          second: "$distribuidor",
        },
        pipeline: [
          {
            $match: {
              createdAt: {
                $gte: new Date(fecha_referencia),
              },
              $expr: {
                $and: [
                  {
                    $eq: ["$idPunto", "$$first"],
                  },
                  {
                    $eq: ["$idDistribuidor", "$$second"],
                  },
                ],
              },
            },
          },
          {
            $facet: {
              pedidos_numero: [
                {
                  $group: {
                    _id: "$idPedido",
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
                    _id: "$idPunto",
                    total: { $sum: "$costoTotalProducto" },
                  },
                },
              ],
              pedidos_productos: [
                {
                  $group: {
                    _id: "$productoId",
                    total: { $sum: 1 },
                  },
                },
              ],
            },
          },
          {
            $project: {
              pedidos_numero: { $size: "$pedidos_numero" },
              pedidos_valor: {
                $arrayElemAt: ["$pedidos_valor.total", 0],
              },
              pedidos_productos: { $size: "$pedidos_productos" },
            },
          },
        ],
        as: "data_compras",
      },
    },
    {
      $lookup: {
        from: "trabajadors",
        localField: "vendedor", //Nombre del campo de la coleccion actual
        foreignField: "_id", //Nombre del campo de la coleccion a relacionar
        pipeline: [
          {
            $match: {
              solicitud_vinculacion: "Aprobado",
            },
          },
          {
            $project: {
              nombre_trabajador: { $concat: ["$nombres", " ", "$apellidos"] },
            },
          },
        ],
        as: "data_trabajadores", //Nombre del campo donde se insertara todos los documentos relacionados
      },
    },
    {
      $lookup: {
        from: "punto_entregas",
        localField: "punto_entrega", //Nombre del campo de la coleccion actual
        foreignField: "_id", //Nombre del campo de la coleccion a relacionar
        pipeline: [
          {
            $lookup: {
              from: Usiario_horecaDB.collection.name, //Nombre de la colecccion a relacionar
              localField: "usuario_horeca", //Nombre del campo de la coleccion actual
              foreignField: "_id", //Nombre del campo de la coleccion a relacionar
              as: "data_horeca", //Nombre del campo donde se insertara todos los documentos relacionados
            },
          },
        ],
        as: "data_punto", //Nombre del campo donde se insertara todos los documentos relacionados
      },
    },
    {
      $unwind: "$data_punto",
    },
    {
      $addFields: {
        data_horeca: {
          $arrayElemAt: ["$data_punto.data_horeca", 0],
        },
      },
    },
    {
      $project: {
        horeca_tipo_negocio: "$data_horeca.tipo_negocio",
        horeca_tipo_usuario: "$data_horeca.tipo_usuario",
        horeca_correo: "$data_horeca.correo",
        horeca_nit: "$data_horeca.nit",
        horeca_telefono: "$data_horeca.empresa_telefono",
        horeca_nombre: "$data_horeca.nombre_establecimiento",
        horeca_razon_social: "$data_horeca.razon_social",
        horeca_celular: "$data_horeca.empresa_telefono2",
        punto_nombre: "$data_punto.nombre",
        punto_pais: "$data_punto.pais",
        punto_departamento: "$data_punto.departamento",
        punto_ciudad: "$data_punto.ciudad",
        punto_direccion: "$data_punto.direccion",
        punto_telefono: "$data_punto.telefono",
        punto_celular: "$data_punto.celular",
        punto_encargado: "$data_punto.informacion_contacto",
        punto_sillas: "$data_punto.sillas",
        punto_domicilios: "$data_punto.domicilios",
        // punto_correo: {
        //   $arrayElemAt: ["$data_punto.informacion_contacto", 2],
        // },
        // punto_telefono_2: {
        //   $arrayElemAt: ["$data_punto.informacion_contacto", 3],
        // },
        vinculacion_estado: "$estado",
        vinculacion_convenio: "$convenio",
        vinculacion_cartera: "$cartera",
        vinculacion_nombre_trabajador: "$data_trabajadores.nombre_trabajador",
        compra_pedidos_numero: {
          $arrayElemAt: ["$data_compras.pedidos_numero", 0],
        },
        compra_pedidos_valor: {
          $arrayElemAt: ["$data_compras.pedidos_valor", 0],
        },
        compra_pedidos_productos: {
          $arrayElemAt: ["$data_compras.pedidos_productos", 0],
        },
      },
    },
    /** Se agrega este campo para un sort no-case-sensitive */
    {
      $addFields: {
        label_to_sort: {
          $toLower: "$punto_nombre",
        },
        label_to_sort_2: {
          $toLower: "$horeca_nombre",
        },
      },
    },
    {
      $sort: { label_to_sort_2: 1, label_to_sort: 1 },
    },
  ]).exec(callback);
};
module.exports.getInformeDistribuidorTablaClientes2 = function (
  req,
  callback
) {
  const d = new Date();
  const fecha_referencia = d.setMonth(d.getMonth() - 3);
  this.aggregate([
    {
      $match: req.query,
    },
    {
      $sort: {
        created: -1
      }
    },
    {
      $skip: req.skip
    },
    {
      $limit: 10
    },
    /** Busca las transacciones entre punto y dist */
    {
      $lookup: {
        from: "reportepedidos",
        let: {
          first: "$punto_entrega",
          second: "$distribuidor",
        },
        pipeline: [
          {
            $match: {
              createdAt: {
                $gte: new Date(fecha_referencia),
              },
              $expr: {
                $and: [
                  {
                    $eq: ["$idPunto", "$$first"],
                  },
                  {
                    $eq: ["$idDistribuidor", "$$second"],
                  },
                ],
              },
            },
          },
          {
            $facet: {
              pedidos_numero: [
                {
                  $group: {
                    _id: "$idPedido",
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
                    _id: "$idPunto",
                    total: { $sum: "$costoTotalProducto" },
                  },
                },
              ],
              pedidos_productos: [
                {
                  $group: {
                    _id: "$productoId",
                    total: { $sum: 1 },
                  },
                },
              ],
            },
          },
          {
            $project: {
              pedidos_numero: { $size: "$pedidos_numero" },
              pedidos_valor: {
                $arrayElemAt: ["$pedidos_valor.total", 0],
              },
              pedidos_productos: { $size: "$pedidos_productos" },
            },
          },
        ],
        as: "data_compras",
      },
    },
    {
      $lookup: {
        from: "trabajadors",
        localField: "vendedor", //Nombre del campo de la coleccion actual
        foreignField: "_id", //Nombre del campo de la coleccion a relacionar
        pipeline: [
          {
            $match: {
              solicitud_vinculacion: "Aprobado",
            },
          },
          {
            $project: {
              nombre_trabajador: { $concat: ["$nombres", " ", "$apellidos"] },
            },
          },
        ],
        as: "data_trabajadores", //Nombre del campo donde se insertara todos los documentos relacionados
      },
    },
    {
      $lookup: {
        from: "punto_entregas",
        localField: "punto_entrega", //Nombre del campo de la coleccion actual
        foreignField: "_id", //Nombre del campo de la coleccion a relacionar
        pipeline: [
          {
            $lookup: {
              from: Usiario_horecaDB.collection.name, //Nombre de la colecccion a relacionar
              localField: "usuario_horeca", //Nombre del campo de la coleccion actual
              foreignField: "_id", //Nombre del campo de la coleccion a relacionar
              as: "data_horeca", //Nombre del campo donde se insertara todos los documentos relacionados
            },
          },
        ],
        as: "data_punto", //Nombre del campo donde se insertara todos los documentos relacionados
      },
    },
    {
      $unwind: "$data_punto",
    },
    {
      $addFields: {
        data_horeca: {
          $arrayElemAt: ["$data_punto.data_horeca", 0],
        },
      },
    },
    {
      $project: {
        createdAt: "$createdAt",
        horeca_tipo_negocio: "$data_horeca.tipo_negocio",
        horeca_tipo_usuario: "$data_horeca.tipo_usuario",
        horeca_correo: "$data_horeca.correo",
        horeca_nit: "$data_horeca.nit",
        horeca_telefono: "$data_horeca.empresa_telefono",
        horeca_nombre: "$data_horeca.nombre_establecimiento",
        horeca_razon_social: "$data_horeca.razon_social",
        horeca_celular: "$data_horeca.empresa_telefono2",
        punto_nombre: "$data_punto.nombre",
        punto_pais: "$data_punto.pais",
        punto_departamento: "$data_punto.departamento",
        punto_ciudad: "$data_punto.ciudad",
        punto_direccion: "$data_punto.direccion",
        punto_telefono: "$data_punto.telefono",
        punto_celular: "$data_punto.celular",
        punto_encargado: "$data_punto.informacion_contacto",
        punto_sillas: "$data_punto.sillas",
        punto_domicilios: "$data_punto.domicilios",
        vinculacion_estado: "$estado",
        vinculacion_convenio: "$convenio",
        vinculacion_cartera: "$cartera",
        vinculacion_nombre_trabajador: "$data_trabajadores.nombre_trabajador",
        compra_pedidos_numero: {
          $arrayElemAt: ["$data_compras.pedidos_numero", 0],
        },
        compra_pedidos_valor: {
          $arrayElemAt: ["$data_compras.pedidos_valor", 0],
        },
        compra_pedidos_productos: {
          $arrayElemAt: ["$data_compras.pedidos_productos", 0],
        },
      },
    },
    /** Se agrega este campo para un sort no-case-sensitive */
    {
      $addFields: {
        label_to_sort: {
          $toLower: "$punto_nombre",
        },
        label_to_sort_2: {
          $toLower: "$horeca_nombre",
        },
      },
    },
    {
      $sort: { label_to_sort_2: 1, label_to_sort: 1 },
    },
    {
      $sort: { createdAt: -1 }
    },
  ]).exec(callback);
};
/** TABLA equipo comercial distribuidor */
module.exports.getInformeDistribuidorTablaEquipComercial = function (
  query,
  callback
) {
  const d = new Date();
  const fecha_referencia = d.setMonth(d.getMonth() - 3);
  this.aggregate([
    {
      $match: {
        estado: "Aprobado",
        distribuidor: new ObjectId(query.idDistribuidor),
      },
    },
    /** Busca las transacciones entre punto y dist */
    {
      $lookup: {
        from: "reportepedidos",
        let: {
          first: "$punto_entrega",
          second: "$distribuidor",
        },
        pipeline: [
          /** Retorna solo las compras entre punto y distribuidor */
          {
            $match: {
              createdAt: {
                $gte: new Date(fecha_referencia),
              },
              $expr: {
                $and: [
                  {
                    $eq: ["$idPunto", "$$first"],
                  },
                  {
                    $eq: ["$idDistribuidor", "$$second"],
                  },
                ],
              },
            },
          },
          {
            $facet: {
              pedidos_numero: [
                {
                  $group: {
                    _id: "$idPedido",
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
                    _id: "$idPunto",
                    total: { $sum: "$costoTotalProducto" },
                  },
                },
              ],
              pedidos_productos: [
                {
                  $group: {
                    _id: "$productoId",
                    total: { $sum: 1 },
                  },
                },
              ],
            },
          },
          {
            $project: {
              pedidos_numero: { $size: "$pedidos_numero" },
              pedidos_valor: {
                $arrayElemAt: ["$pedidos_valor.total", 0],
              },
              pedidos_productos: { $size: "$pedidos_productos" },
            },
          },
        ],
        as: "data_compras",
      },
    },
    /** Busca el nombre de los vendedores de cada punto-dist */
    {
      $lookup: {
        from: "trabajadors",
        localField: "vendedor", //Nombre del campo de la coleccion actual
        foreignField: "_id", //Nombre del campo de la coleccion a relacionar
        pipeline: [
          {
            $match: {
              solicitud_vinculacion: "Aprobado",
            },
          },
          {
            $project: {
              trabajador_id: "$_id",
              trabajador_tipo: "$tipo_trabajador",
              trabajador_estado: "$solicitud_vinculacion",
              trabajador_nombres: { $concat: ["$nombres", " ", "$apellidos"] },
              trabajador_tipo_documento: "$tipo_documento",
              trabajador_numero_documento: "$numero_documento",
              trabajador_correo: "$correo",
              trabajador_telefono: "$telefono",
              trabajador_celular: "$celular",
            },
          },
        ],
        as: "data_trabajadores", //Nombre del campo donde se insertara todos los documentos relacionados
      },
    },
    /** Le agrega al trabajador el total de pedidos realizados */
    {
      $addFields: {
        "data_trabajadores.pedidos_numero": {
          $arrayElemAt: ["$data_compras.pedidos_numero", 0],
        },
        "data_trabajadores.pedidos_valor": {
          $arrayElemAt: ["$data_compras.pedidos_valor", 0],
        },
        "data_trabajadores.pedidos_productos": {
          $arrayElemAt: ["$data_compras.pedidos_productos", 0],
        },
      },
    },
    {
      $unwind: "$data_trabajadores",
    },
    {
      $replaceRoot: {
        newRoot: "$data_trabajadores",
      },
    },
    {
      $group: {
        _id: "$trabajador_id",
        trabajador_tipo: { $first: "$trabajador_tipo" },
        trabajador_estado: { $first: "$trabajador_estado" },
        trabajador_nombres: { $first: "$trabajador_nombres" },
        trabajador_tipo_documento: { $first: "$trabajador_tipo_documento" },
        trabajador_numero_documento: { $first: "$trabajador_numero_documento" },
        trabajador_correo: { $first: "$trabajador_correo" },
        trabajador_telefono: { $first: "$trabajador_telefono" },
        trabajador_celular: { $first: "$trabajador_celular" },
        pedidos_establecimientos_asignados: { $sum: 1 },
        pedidos_numero: { $sum: "$pedidos_numero" },
        pedidos_valor: { $sum: "$pedidos_valor" },
        pedidos_productos: { $sum: "$pedidos_productos" },
      },
    },
    /** Se agrega este campo para un sort no-case-sensitive */
    {
      $addFields: {
        label_to_sort: {
          $toLower: "$trabajador_nombres",
        },
      },
    },
    {
      $sort: { label_to_sort: 1 },
    },
  ]).exec(callback);
};

module.exports.distHomeEstablecimientosXCiudad = function (req, callback) {
  this.aggregate([
    {
      $match: {
        estado: "Aprobado",
        distribuidor: new ObjectId(req.trim()),
      },
    },
    {
      $lookup: {
        from: Puntos_entregaBD, //Nombre de la colecccion a relacionar
        localField: "punto_entrega", //Nombre del campo de la coleccion actual
        foreignField: "_id", //Nombre del campo de la coleccion a relacionar
        as: "punto_entrega", //Nombre del campo donde se insertara todos los documentos relacionados
      },
    },
    {
      $unwind: "$punto_entrega",
    },
    {
      $replaceRoot: {
        newRoot: "$punto_entrega",
      },
    },
    {
      $group: {
        _id: "$usuario_horeca",
        usuario_horeca: { $first: "$usuario_horeca" },
      },
    },
    {
      $lookup: {
        from: "usuario_horecas", //Nombre de la colecccion a relacionar
        localField: "usuario_horeca", //Nombre del campo de la coleccion actual
        foreignField: "_id", //Nombre del campo de la coleccion a relacionar
        as: "usuario_horeca", //Nombre del campo donde se insertara todos los documentos relacionados
      },
    },
    {
      $unwind: "$usuario_horeca",
    },
    {
      $replaceRoot: {
        newRoot: "$usuario_horeca",
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
};
module.exports.distHomeEstablecimientosXTiposNegocio = function (
  req,
  callback
) {
  this.aggregate([
    {
      $match: {
        estado: "Aprobado",
        distribuidor: new ObjectId(req.trim()),
      },
    },
    {
      $lookup: {
        from: Puntos_entregaBD, //Nombre de la colecccion a relacionar
        localField: "punto_entrega", //Nombre del campo de la coleccion actual
        foreignField: "_id", //Nombre del campo de la coleccion a relacionar
        as: "punto_entrega", //Nombre del campo donde se insertara todos los documentos relacionados
      },
    },
    {
      $unwind: "$punto_entrega",
    },
    {
      $replaceRoot: {
        newRoot: "$punto_entrega",
      },
    },
    {
      $group: {
        _id: "$usuario_horeca",
        usuario_horeca: { $first: "$usuario_horeca" },
      },
    },
    {
      $lookup: {
        from: "usuario_horecas", //Nombre de la colecccion a relacionar
        localField: "usuario_horeca", //Nombre del campo de la coleccion actual
        foreignField: "_id", //Nombre del campo de la coleccion a relacionar
        as: "usuario_horeca", //Nombre del campo donde se insertara todos los documentos relacionados
      },
    },
    {
      $unwind: "$usuario_horeca",
    },
    {
      $replaceRoot: {
        newRoot: "$usuario_horeca",
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
};
module.exports.distHomeEstablecimientosXCategoria = function (req, callback) {
  this.aggregate([
    {
      $match: {
        estado: "Aprobado",
        distribuidor: new ObjectId(req.trim()),
      },
    },
    {
      $lookup: {
        from: Puntos_entregaBD, //Nombre de la colecccion a relacionar
        localField: "punto_entrega", //Nombre del campo de la coleccion actual
        foreignField: "_id", //Nombre del campo de la coleccion a relacionar
        as: "punto_entrega", //Nombre del campo donde se insertara todos los documentos relacionados
      },
    },
    {
      $unwind: "$punto_entrega",
    },
    {
      $replaceRoot: {
        newRoot: "$punto_entrega",
      },
    },
    {
      $group: {
        _id: "$usuario_horeca",
        usuario_horeca: { $first: "$usuario_horeca" },
      },
    },
    {
      $lookup: {
        from: "usuario_horecas", //Nombre de la colecccion a relacionar
        localField: "usuario_horeca", //Nombre del campo de la coleccion actual
        foreignField: "_id", //Nombre del campo de la coleccion a relacionar
        as: "usuario_horeca", //Nombre del campo donde se insertara todos los documentos relacionados
      },
    },
    {
      $unwind: "$usuario_horeca",
    },
    {
      $replaceRoot: {
        newRoot: "$usuario_horeca",
      },
    },
    {
      $group: {
        _id: "$tipo_establecimiento",
        total: { $sum: 1 },
      },
    },
    {
      $sort: { total: -1 },
    },
  ]).exec(callback);
};
module.exports.distHomeSillasXCiudad = function (req, callback) {
  this.aggregate([
    {
      $match: {
        estado: "Aprobado",
        distribuidor: new ObjectId(req.trim()),
      },
    },
    {
      $lookup: {
        from: Puntos_entregaBD, //Nombre de la colecccion a relacionar
        localField: "punto_entrega", //Nombre del campo de la coleccion actual
        foreignField: "_id", //Nombre del campo de la coleccion a relacionar
        as: "punto_entrega", //Nombre del campo donde se insertara todos los documentos relacionados
      },
    },
    {
      $unwind: "$punto_entrega",
    },
    {
      $replaceRoot: {
        newRoot: "$punto_entrega",
      },
    },
    {
      $group: {
        _id: "$ciudad",
        sillas: { $sum: "$sillas" },
      },
    },
    {
      $sort: { total: -1 },
    },
  ]).exec(callback);
};
module.exports.distHomeSillasXTipoNegocio = function (req, callback) {
  this.aggregate([
    {
      $match: {
        estado: "Aprobado",
        distribuidor: new ObjectId(req.trim()),
      },
    },
    {
      $lookup: {
        from: Puntos_entregaBD, //Nombre de la colecccion a relacionar
        localField: "punto_entrega", //Nombre del campo de la coleccion actual
        foreignField: "_id", //Nombre del campo de la coleccion a relacionar
        as: "punto_entrega", //Nombre del campo donde se insertara todos los documentos relacionados
      },
    },
    {
      $unwind: "$punto_entrega",
    },
    {
      $replaceRoot: {
        newRoot: "$punto_entrega",
      },
    },
    {
      $group: {
        _id: "$usuario_horeca",
        sillas: { $sum: "$sillas" },
        usuario_horeca: { $first: "$usuario_horeca" },
      },
    },
    {
      $lookup: {
        from: "usuario_horecas", //Nombre de la colecccion a relacionar
        localField: "usuario_horeca", //Nombre del campo de la coleccion actual
        foreignField: "_id", //Nombre del campo de la coleccion a relacionar
        as: "usuario_horeca", //Nombre del campo donde se insertara todos los documentos relacionados
      },
    },
    {
      $group: {
        _id: {
          $arrayElemAt: ["$usuario_horeca.tipo_negocio", 0],
        },
        total: { $sum: "$sillas" },
      },
    },
    {
      $sort: { total: -1 },
    },
  ]).exec(callback);
};
module.exports.distHomeSillasXCategoria = function (req, callback) {
  this.aggregate([
    {
      $match: {
        estado: "Aprobado",
        distribuidor: new ObjectId(req.trim()),
      },
    },
    {
      $lookup: {
        from: Puntos_entregaBD, //Nombre de la colecccion a relacionar
        localField: "punto_entrega", //Nombre del campo de la coleccion actual
        foreignField: "_id", //Nombre del campo de la coleccion a relacionar
        as: "punto_entrega", //Nombre del campo donde se insertara todos los documentos relacionados
      },
    },
    {
      $unwind: "$punto_entrega",
    },
    {
      $replaceRoot: {
        newRoot: "$punto_entrega",
      },
    },
    {
      $group: {
        _id: "$usuario_horeca",
        sillas: { $sum: "$sillas" },
        usuario_horeca: { $first: "$usuario_horeca" },
      },
    },
    {
      $lookup: {
        from: "usuario_horecas", //Nombre de la colecccion a relacionar
        localField: "usuario_horeca", //Nombre del campo de la coleccion actual
        foreignField: "_id", //Nombre del campo de la coleccion a relacionar
        as: "usuario_horeca", //Nombre del campo donde se insertara todos los documentos relacionados
      },
    },
    {
      $group: {
        _id: {
          $arrayElemAt: ["$usuario_horeca.tipo_establecimiento", 0],
        },
        total: { $sum: "$sillas" },
      },
    },
    {
      $sort: { total: -1 },
    },
  ]).exec(callback);
};
module.exports.getDisVinculacionHoreca = function (data, callback) {
  const skip = (data.pagina - 1) * 10;
  const totalPaginas = Math.ceil(data.totalVinculaciones / 10);
  const threeMonthsAgo = moment().subtract(3, 'months').toDate();
  const today = new Date();
  this.aggregate([
    {
      $match: {
        distribuidor: new ObjectId(data.distribuidor),
      },
    },
    {
      $skip: skip
    },
    {
      $limit: 10
    },
    {
      $lookup: {
        from: "reportepedidos", //Nombre de la colecccion a relacionar
        localField: "punto_entrega", //Nombre del campo de la coleccion actual
        foreignField: "idPunto", //Nombre del campo de la coleccion a relacionar
        as: "dataPedidos3Meses", //Nombre del campo donde se insertara todos los documentos relacionados
        pipeline: [
          {
            $match: {
              idDistribuidor: new ObjectId(data.distribuidor),
              createdAt: { $gte: threeMonthsAgo, $lt: today },
              estaActTraking: { $in: ['Entregado', 'Recibido', 'Calificado'] }

            }
          },
          {
            $facet: {
              totalCompra: [
                {
                  $group: {
                    _id: "$idPedido",
                    total: { $last: "$subtotalCompra" },
                  },
                },
                {
                  $addFields: {
                    bandera: "pedido"
                  }
                },
                {
                  $group: {
                    _id: "$bandera",
                    count: { $sum: "$total" },
                  },
                },
              ],
              totalPedidos: [
                {
                  $group: {
                    _id: "$idPedido",
                    count: { $sum: 1 },
                  },
                },
                {
                  $addFields: {
                    bandera: "pedido"
                  }
                },
                {
                  $group: {
                    _id: "$bandera",
                    count: { $sum: 1 },
                  },
                },



              ],
              totalProductos: [
                {
                  $group: {
                    _id: "$codigoFeatProducto",
                    count: { $sum: 1 },
                  },
                },
                {
                  $addFields: {
                    bandera: "producto"
                  }
                },
                {
                  $group: {
                    _id: "$bandera",
                    count: { $sum: 1 },
                  },
                },
              ],
            },
          },
        ]
      },
    },
    {
      $lookup: {
        from: Puntos_entregaBD, //Nombre de la colecccion a relacionar
        localField: "punto_entrega", //Nombre del campo de la coleccion actual
        foreignField: "_id", //Nombre del campo de la coleccion a relacionar
        as: "punto_entrega", //Nombre del campo donde se insertara todos los documentos relacionados
        pipeline: [
          {
            $lookup: {
              from: "usuario_horecas", //Nombre de la colecccion a relacionar
              localField: "usuario_horeca", //Nombre del campo de la coleccion actual
              foreignField: "_id", //Nombre del campo de la coleccion a relacionar
              as: "dataHoreca", //Nombre del campo donde se insertara todos los documentos relacionados
              pipeline: [
                {
                  $project: {
                    tipoNegocio: "$tipo_negocio",
                    tipoPersona: "$tipo_usuario",
                    nit: "$nit",
                    razon_social: "$razon_social",
                    nombre: "$nombre_establecimiento",
                    propietario_correo: "$propietario_correo",
                    propietario_telefono: "$propietario_telefono",

                    id: "$_id"
                  }
                },
                {
                  $lookup: {
                    from: 'trabajadors', //Nombre de la colecccion a relacionar
                    localField: "_id", //Nombre del campo de la coleccion actual
                    foreignField: "usuario_horeca", //Nombre del campo de la coleccion a relacionar
                    as: "dataTrabajadorPunto", //Nombre del campo donde se insertara todos los documentos relacionados
                  }
                },
              ]
            },
          }, {
            $addFields: {
              nombreEncargado: { $arrayElemAt: ["$informacion_contacto", 0] },
              apellidoEncargado: { $arrayElemAt: ["$informacion_contacto", 1] },
              emailEncargado: { $arrayElemAt: ["$informacion_contacto", 2] },
              telefonoEncargado: { $arrayElemAt: ["$informacion_contacto", 3] },

            }
          }
        ]
      },
    },
    {
      $lookup: {
        from: "trabajadors", //Nombre de la colecccion a relacionar
        localField: "vendedor", //Nombre del campo de la coleccion actual
        foreignField: "_id", //Nombre del campo de la coleccion a relacionar
        as: "equipo_comercial", //Nombre del campo donde se insertara todos los documentos relacionados
        pipeline: [
          {
            $project: {
              nombre: { $concat: ["$nombres", " ", "$apellidos"] },
              tipo_trabajador: "$tipo_trabajador"
            }
          }
        ]
      },
    },
    {
      $project: {
        _id: "$_id",
        dataPedidos3Meses: "$dataPedidos3Meses",
        estado_vinculacion: "$estado",
        tipo_negocio: { $last: "$punto_entrega.dataHoreca.tipoNegocio" },
        nombre: { $last: "$punto_entrega.dataHoreca.nombre" },
        razon_social: { $last: "$punto_entrega.dataHoreca.razon_social" },
        tipo_persona: { $last: "$punto_entrega.dataHoreca.tipoPersona" },
        nit: { $last: "$punto_entrega.dataHoreca.nit" },
        pais: { $last: "$punto_entrega.pais" },
        ciudad: { $last: "$punto_entrega.ciudad" },
        departamento: { $last: "$punto_entrega.departamento" },
        punto_entrega: { $last: "$punto_entrega.nombre" },
        direccion: { $last: "$punto_entrega.direccion" },
        sillas: { $last: "$punto_entrega.sillas" },
        domicilio: { $last: "$punto_entrega.domicilios" },
        precio_especial: "$convenio",
        estadoCartera: "$pazysalvo",
        equipo: "$equipo_comercial",

        emailPropietario: { $last: "$punto_entrega.dataHoreca.propietario_correo" },
        telPropietario: { $last: "$punto_entrega.dataHoreca.propietario_telefono" },

        dataHoreca: { $last: "$punto_entrega.dataHoreca" },
        emailEncargado: { $last: "$punto_entrega.emailEncargado" },
        telefonoEncargado: { $last: "$punto_entrega.telefonoEncargado" },
        nombreEncargado: { $concat: [{ $last: "$punto_entrega.nombreEncargado" }, " ", { $last: "$punto_entrega.apellidoEncargado" }] },
        totalPedidos3MesesCompra: { $last: "$dataPedidos3Meses.totalCompra.count" },
        totalProductos3Meses: { $last: "$dataPedidos3Meses.totalProductos.count" },
        totalPedidos3Meses: { $last: "$dataPedidos3Meses.totalPedidos.count" },
        dataTrabajadorPunto: { $arrayElemAt: ["$punto_entrega.dataHoreca.dataTrabajadorPunto", 0] },
      }
    }
  ]).exec(callback);
};
module.exports.distribuidoresVinculadosDetalle = function (data, callback) {
  this.aggregate([
    {
      $match: {
        _id: new ObjectId(data.punto),
      },
    },
    {
      $lookup: {
        from: "reportepedidos", //Nombre de la colecccion a relacionar
        localField: "punto_entrega", //Nombre del campo de la coleccion actual
        foreignField: "idPunto", //Nombre del campo de la coleccion a relacionar
        as: "dataPedidos3Meses", //Nombre del campo donde se insertara todos los documentos relacionados
        pipeline: [
          {
            $facet: {
              pedidosEntregados: [
                {
                  $match: {
                    idDistribuidor: new ObjectId(data.distribuidor)
                  }
                },
                {
                  $match:
                  {
                    $or: [
                      { estaActTraking: "Entregado" },
                      { estaActTraking: "Recibido" },
                      { estaActTraking: "Calificado" }]
                  }
                },
              ],
              promedioMensual: [
                {
                  $match: {
                    idDistribuidor: new ObjectId(data.distribuidor)
                  }
                },
                {
                  $match: { $or: [{ estaActTraking: "Entregado" }, { estaActTraking: "Recibido" }, { estaActTraking: "Calificado" }] }
                },
                {
                  $group: {
                    _id: "$idPedido",
                    total: { $sum: "$totalCompra" },
                    cantidad: { $sum: 1 }
                  },
                },
                {
                  $addFields: {
                    bandera: "producto"
                  }
                },
                {
                  $group: {
                    _id: "$bandera",
                    total: { $sum: "$total" },
                    cantidad: { $sum: "$cantidad" },
                  },
                },
                {
                  $addFields: {
                    resta: { $divide: ["$total", "$cantidad"] }
                  }
                },
              ],
              referenciasXpedido: [
                {
                  $match: {
                    idDistribuidor: new ObjectId(data.distribuidor)
                  }
                },
                {
                  $match: { $or: [{ estaActTraking: "Entregado" }, { estaActTraking: "Recibido" }, { estaActTraking: "Calificado" }] }
                },
                {
                  $group: {
                    _id: { pedido: "$idPedido", codFeat: "$codigoFeatProducto" },
                    total: { $sum: 1 }
                  },
                },
                {
                  $group: {
                    _id: "$_id.pedido",
                    total: { $sum: 1 }
                  },
                },
                {
                  $addFields: {
                    bandera: "producto"
                  }
                },
                {
                  $group: {
                    _id: "$bandera",
                    total: { $sum: "$total" },
                    cantidad: { $sum: 1 }
                  },
                },
                {
                  $addFields: {
                    promedio: { $divide: ["$total", "$cantidad"] }
                  }
                },
                {
                  $project: {
                    promedio: "$promedio"
                  }
                }
              ],
            },
          },
        ]
      },
    },
    {
      $lookup: {
        from: Puntos_entregaBD, //Nombre de la colecccion a relacionar
        localField: "punto_entrega", //Nombre del campo de la coleccion actual
        foreignField: "_id", //Nombre del campo de la coleccion a relacionar
        as: "punto_entrega", //Nombre del campo donde se insertara todos los documentos relacionados
        pipeline: [
          {
            $lookup: {
              from: "trabajadors", //Nombre de la colecccion a relacionar
              localField: "_id", //Nombre del campo de la coleccion actual
              foreignField: "puntos_entrega", //Nombre del campo de la coleccion a relacionar
              as: "dataTrabajadorsVinculado", //Nombre del campo donde se insertara todos los documentos relacionados
              pipeline: [
                {
                  $match: {
                    solicitud_vinculacion: "Aprobado"
                  }
                }
              ]
            },
          },
          {
            $lookup: {
              from: "usuario_horecas", //Nombre de la colecccion a relacionar
              localField: "usuario_horeca", //Nombre del campo de la coleccion actual
              foreignField: "_id", //Nombre del campo de la coleccion a relacionar
              as: "dataHoreca", //Nombre del campo donde se insertara todos los documentos relacionados
              pipeline: [
                {
                  $project: {
                    tipoNegocio: "$tipo_negocio",
                    tipoPersona: "$tipo_usuario",
                    nit: "$nit",
                    razon_social: "$razon_social",
                    nombre: "$nombre_establecimiento",
                    propietario_tipo_documento: "$propietario_tipo_documento",
                    propietario_numero_documento: "$propietario_numero_documento",
                    propietario_nombres: "$propietario_nombres",
                    propietario_apellidos: "$propietario_apellidos",
                    propietario_telefono: "$propietario_telefono",
                    propietario_correo: "$propietario_correo",
                    logo: "$logo",
                    id: "$_id"
                  }
                },
                {
                  $lookup: {
                    from: 'trabajadors', //Nombre de la colecccion a relacionar
                    localField: "_id", //Nombre del campo de la coleccion actual
                    foreignField: "usuario_horeca", //Nombre del campo de la coleccion a relacionar
                    as: "dataTrabajadorPunto", //Nombre del campo donde se insertara todos los documentos relacionados
                  }
                },

                {
                  $lookup: {
                    from: "documentos_usuario_solicitantes", //Nombre de la colecccion a relacionar
                    localField: "_id", //Nombre del campo de la coleccion actual
                    foreignField: "usuario", //Nombre del campo de la coleccion a relacionar
                    as: "documentosEstablecimientos", //Nombre del campo donde se insertara todos los documentos relacionados
                  },
                },
              ]
            },
          },
          {
            $lookup: {
              from: Puntos_entregaBD, //Nombre de la colecccion a relacionar
              localField: "usuario_horeca", //Nombre del campo de la coleccion actual
              foreignField: "usuario_horeca", //Nombre del campo de la coleccion a relacionar
              as: "dataPuntos", //Nombre del campo donde se insertara todos los documentos relacionados
              pipeline: [
                {
                  $match: {
                    estado: 'Activo'
                  }
                },
                {
                  $project: {
                    nombre: "$nombre",
                    sillas: "$sillas",
                    direccion: "$direccion",
                    horario: "$horario",
                  }
                }
              ]
            },
          },
          {
            $addFields: {
              nombreEncargado: { $arrayElemAt: ["$informacion_contacto", 0] },
              apellidoEncargado: { $arrayElemAt: ["$informacion_contacto", 1] },
              emailEncargado: { $arrayElemAt: ["$informacion_contacto", 2] },
              telefonoEncargado: { $arrayElemAt: ["$informacion_contacto", 3] },

            }
          },

        ]
      },
    },
    {
      $lookup: {
        from: "trabajadors", //Nombre de la colecccion a relacionar
        localField: "vendedor", //Nombre del campo de la coleccion actual
        foreignField: "_id", //Nombre del campo de la coleccion a relacionar
        as: "equipo_comercial", //Nombre del campo donde se insertara todos los documentos relacionados
        pipeline: [
          {
            $project: {
              nombre: { $concat: ["$nombres", " ", "$apellidos"] },
              tipo_trabajador: "$tipo_trabajador"
            }
          }
        ]
      },
    },
    {
      $lookup: {
        from: "trabajadors", //Nombre de la colecccion a relacionar
        localField: "distribuidor", //Nombre del campo de la coleccion actual
        foreignField: "distribuidor", //Nombre del campo de la coleccion a relacionar
        as: "trabajadoresDistribuidor", //Nombre del campo donde se insertara todos los documentos relacionados
        pipeline: [
          {
            $match: { solicitud_vinculacion: "Aprobado" }
          }
        ]
      },
    },
    {
      $project: {
        pedidos: "$dataPedidos3Meses",
        logo: { $arrayElemAt: ['$punto_entrega.dataHoreca.logo', 0] },
        nombrePunto: { $arrayElemAt: ['$punto_entrega.nombre', 0] },
        nombre: { $arrayElemAt: ['$punto_entrega.dataHoreca.nombre', 0] },
        nit: { $arrayElemAt: ['$punto_entrega.dataHoreca.nit', 0] },
        encargadoNombre: "$punto_entrega.nombreEncargado",
        apellidoEncargado: "$punto_entrega.apellidoEncargado",
        dataHorecaCompleta: { $arrayElemAt: ['$punto_entrega.dataHoreca', 0] },
        tipoNegocio: { $arrayElemAt: ['$punto_entrega.dataHoreca.tipoNegocio', 0] },
        sillas: '$punto_entrega.sillas',
        integrantesEquipo: '$punto_entrega',
        pedidosEntregados: "$dataPedidos3Meses.pedidosEntregados",
        promedioMensual: { $arrayElemAt: ['$dataPedidos3Meses.promedioMensual.resta', 0] },
        referenciasXpedido: { $arrayElemAt: ['$dataPedidos3Meses.referenciasXpedido.promedio', 0] },
        servicioDomicilio: "$punto_entrega.domicilios",
        ciudadContacto: "$punto_entrega.ciudad",
        direccionContacto: "$punto_entrega.direccion",
        telefonoContacto: "$punto_entrega.telefonoEncargado",
        emailContacto: "$punto_entrega.emailEncargado",
        dataPropietarios: { $arrayElemAt: ["$punto_entrega.dataHoreca.dataTrabajadorPunto", 0] },
        nombrePropietario: "$punto_entrega.dataHoreca.propietario_nombres",
        telefonoPropietario: "$punto_entrega.dataHoreca.propietario_telefono",
        emailPropietario: "$punto_entrega.dataHoreca.propietario_correo",
        otrosPuntos: "$punto_entrega.dataPuntos",
        convenio: "$convenio",
        pazYsalvo: "$pazysalvo",
        vendedor: "$vendedor",
        vendedoresData: "",
        trabajadoresDistribuidor: "$trabajadoresDistribuidor",
        documentos: { $arrayElemAt: ["$punto_entrega.dataHoreca.documentosEstablecimientos", 0] },
      }
    }
  ]).exec(callback);
};

module.exports.getAllPedidosOnMyVinculation = async function (data, callback) {
  const skip = (data.pagina - 1) * 10;
  const idVendedor = new ObjectId(data.idUser);
  const idDist = new ObjectId(data.idDistribuidor);
  let queryPedido
  if (data.tab === 'curso') {
    switch (data.estado) {
      case 'pendientes':
        queryPedido = [
          { estado: "Aprobado Interno" },
        ]
        break;
      case 'aprobados':
        queryPedido = [
          { estado: "Aprobado Externo" },
        ]
        break;
      case 'alistamiento':
        queryPedido = [
          { estado: "Alistamiento" },
        ]
        break;
      case 'despachado':
        queryPedido = [
          { estado: "Despachado" },
        ]
        break;
      default:
        queryPedido = [
          { estado: "Aprobado Interno" },
          { estado: "Aprobado Externo" },
          { estado: "Alistamiento" },
          { estado: "Despachado" },
        ]
    }
  } else {
    switch (data.estado) {
      case 'entregados':
        queryPedido = [
          { estado: "Entregado" },
        ]
        break;
      case 'recibido':
        queryPedido = [
          { estado: "Recibido" },
        ]
        break;
      case 'calificados':
        queryPedido = [
          { estado: "Calificado" },
        ]
        break;
      case 'rechazado':
        queryPedido = [
          { estado: "Rechazado" },
        ]
        break;
      case 'cancelados':
        queryPedido = [
          { estado: "Cancelado por horeca" },
          { estado: "Cancelado por distribuidor" },
        ]
        break;
      default:
        queryPedido = [
          { estado: "Entregado" },
          { estado: "Recibido" },
          { estado: "Calificado" },
          { estado: "Rechazado" },
          { estado: "Cancelado por horeca" },
          { estado: "Cancelado por distribuidor" },
        ]
    }
  }
  let matchQuery
  if (data.trabajador && data.trabajador !== '') {
    const user = await TrabajadorDist.findById(data.trabajador);
    if (user.tipo_trabajador === 'PROPIETARIO/REP LEGAL' || data.tipoUser === 'ADMINISTRADOR') {
      matchQuery = {
        distribuidor: idDist,
      }
    } else {
      matchQuery = {
        distribuidor: idDist,
        vendedor: new ObjectId(data.trabajador)
      }
    }

  } else {
    if (data.tipoUser === 'PROPIETARIO/REP LEGAL' || data.tipoUser === 'ADMINISTRADOR') {
      matchQuery = {
        distribuidor: idDist
      }
    } else {
      matchQuery = {
        distribuidor: idDist,
        vendedor: idVendedor
      }
    }
  }
  let consulta = [
    {
      $match: matchQuery
    },
    {
      $lookup: {
        from: "pedidos", // Nombre de la colección de pedidos
        localField: "punto_entrega",
        foreignField: "punto_entrega",
        as: "pedidos",
        pipeline: [

          {
            $match: {
              $and: [ // Usamos $and para combinar las condiciones
                { $or: queryPedido }, // Filtro por estados
                { distribuidor: idDist } // Filtro por distribuidor
              ]
            },
          },
          {
            $sort: { createdAt: -1 }
          },
          {

            $lookup: {
              from: "usuario_horecas", // Nombre de la colección de pedidos
              localField: "usuario_horeca",
              foreignField: "_id",
              as: "dataEstablecimiento",
            }
          },
          {

            $lookup: {
              from: "punto_entregas", // Nombre de la colección de pedidos
              localField: "punto_entrega",
              foreignField: "_id",
              as: "dataPunto",
            }
          },
          {

            $lookup: {
              from: "pedido_trackings", // Nombre de la colección de pedidos
              localField: "_id",
              foreignField: "pedido",
              as: "dataTraking",
            }
          },
          {

            $lookup: {
              from: "codigos_descuento_generados", // Nombre de la colección de pedidos
              localField: "codigo_descuento",
              foreignField: "_id",
              as: "dataCodigos",
            }
          },

        ]
      }
    },
    {
      $lookup: {
        from: "trabajadors", // Nombre de la colección de pedidos
        localField: "vendedor",
        foreignField: "_id",
        as: "dataVendedores",
        pipeline: [
          {
            $project: {
              nombres: { $concat: ["$nombres", " ", "$apellidos"] }
            }
          }
        ]
      }
    },
    {
      $unwind: "$pedidos"
    },
    {
      $sort: { 'pedidos.createdAt': -1 },
    },
  ];
  if (data.buscador && data.buscador != '') {
    const filtroLimpio = data.buscador.trim(); // Elimina espacios iniciales/finales
    const palabras = filtroLimpio.split(/\s+/); // Divide el texto en palabras por espacios
    const regexBusqueda = palabras.map(palabra => `(?=.*${palabra})`).join('') + '.*'; // Crea el regex: "(?=.*palabra1)(?=.*palabra2).*"
    let texto_busqueda = {
      $match: {
        $or: [
          {
            $expr: {
              $regexMatch: {
                input: { $toString: "$pedidos.id_pedido" }, // Convierte a string
                regex: regexBusqueda,
                options: "i"
              }
            }
          },
          { "pedidos.dataEstablecimiento.nombre_establecimiento": { $regex: regexBusqueda, $options: "i" } },
          { "pedidos.dataEstablecimiento.razon_social": { $regex: regexBusqueda, $options: "i" } },
          { "pedidos.dataEstablecimiento.nit": { $regex: regexBusqueda, $options: "i" } },
          { "pedidos.dataPunto.nombre": { $regex: regexBusqueda, $options: "i" } },
        ]
      }
    };
    consulta.push(texto_busqueda);
  }

  consulta.push(
    {
      $skip: skip
    },
    {
      $limit: 10
    },
    {
      $project: {
        _id: "$pedidos._id",
        estadoPedido: "$pedidos.estado",
        valorPedido: "$pedidos.total_pedido",
        metodo_pago: "$pedidos.metodo_pago",
        establecimiento: "$pedidos.dataEstablecimiento.nombre_establecimiento",
        punto: "$pedidos.dataPunto.nombre",
        tipo_persona: "$pedidos.dataEstablecimiento.tipo_usuario",
        nit: "$pedidos.dataEstablecimiento.nit",
        pais: "$pedidos.dataPunto.pais",
        departamento: "$pedidos.dataPunto.departamento",
        ciudad: "$pedidos.dataPunto.ciudad",
        tipo_negocio: "$pedidos.dataEstablecimiento.tipo_negocio",
        fecha_pedido: "$pedidos.createdAt",
        cant_productos: { $size: "$pedidos.productos" },
        puntos_redimidos: "$pedidos.puntos_redimidos",
        codigo_descuento: "$pedidos.codigo_descuento",
        id_pedido: "$pedidos.id_pedido",
        dataVendedores: "$dataVendedores",
        traking: "$pedidos.dataTraking",
        dataCodigos: "$pedidos.dataCodigos",
      }
    },
  )
  this.aggregate(consulta).exec(callback);
};
module.exports.getAllPedidosOnMyVinculationCloneCount = async function (data, callback) {
  const skip = (data.pagina - 1) * 10;
  const idVendedor = new ObjectId(data.idUser);
  const idDist = new ObjectId(data.idDistribuidor);
  let queryPedido
  if (data.tab === 'curso') {
    switch (data.estado) {
      case 'pendientes':
        queryPedido = [
          { estado: "Aprobado Interno" },
        ]
        break;
      case 'aprobados':
        queryPedido = [
          { estado: "Aprobado Externo" },
        ]
        break;
      case 'alistamiento':
        queryPedido = [
          { estado: "Alistamiento" },
        ]
        break;
      case 'despachado':
        queryPedido = [
          { estado: "Despachado" },
        ]
        break;
      default:
        queryPedido = [
          { estado: "Aprobado Interno" },
          { estado: "Aprobado Externo" },
          { estado: "Alistamiento" },
          { estado: "Despachado" },
        ]
    }
  } else {
    switch (data.estado) {
      case 'entregados':
        queryPedido = [
          { estado: "Entregado" },
        ]
        break;
      case 'recibido':
        queryPedido = [
          { estado: "Recibido" },
        ]
        break;
      case 'calificados':
        queryPedido = [
          { estado: "Calificado" },
        ]
        break;
      case 'rechazado':
        queryPedido = [
          { estado: "Rechazado" },
        ]
        break;
      case 'cancelados':
        queryPedido = [
          { estado: "Cancelado por horeca" },
          { estado: "Cancelado por distribuidor" },
        ]
        break;
      default:
        queryPedido = [
          { estado: "Entregado" },
          { estado: "Recibido" },
          { estado: "Calificado" },
          { estado: "Rechazado" },
          { estado: "Cancelado por horeca" },
          { estado: "Cancelado por distribuidor" },
        ]
    }
  }
  let matchQuery
  if (data.trabajador && data.trabajador !== '') {
    const user = await TrabajadorDist.findById(data.trabajador);
    if (user.tipo_trabajador === 'PROPIETARIO/REP LEGAL' || data.tipoUser === 'ADMINISTRADOR') {
      matchQuery = {
        distribuidor: idDist,
      }
    } else {
      matchQuery = {
        distribuidor: idDist,
        vendedor: new ObjectId(data.trabajador)
      }
    }

  } else {
    if (data.tipoUser === 'PROPIETARIO/REP LEGAL' || data.tipoUser === 'ADMINISTRADOR') {
      matchQuery = {
        distribuidor: idDist
      }
    } else {
      matchQuery = {
        distribuidor: idDist,
        vendedor: idVendedor
      }
    }
  }
  let consulta = [
    {
      $match: matchQuery
    },
    {
      $lookup: {
        from: "pedidos", // Nombre de la colección de pedidos
        localField: "punto_entrega",
        foreignField: "punto_entrega",
        as: "pedidos",
        pipeline: [

          {
            $match: {
              $and: [ // Usamos $and para combinar las condiciones
                { $or: queryPedido }, // Filtro por estados
                { distribuidor: idDist } // Filtro por distribuidor
              ]
            },
          },
          {
            $sort: { createdAt: -1 }
          },
          {

            $lookup: {
              from: "usuario_horecas", // Nombre de la colección de pedidos
              localField: "usuario_horeca",
              foreignField: "_id",
              as: "dataEstablecimiento",
            }
          },
          {

            $lookup: {
              from: "punto_entregas", // Nombre de la colección de pedidos
              localField: "punto_entrega",
              foreignField: "_id",
              as: "dataPunto",
            }
          },
          {

            $lookup: {
              from: "pedido_trackings", // Nombre de la colección de pedidos
              localField: "_id",
              foreignField: "pedido",
              as: "dataTraking",
            }
          },
          {

            $lookup: {
              from: "codigos_descuento_generados", // Nombre de la colección de pedidos
              localField: "codigo_descuento",
              foreignField: "_id",
              as: "dataCodigos",
            }
          },

        ]
      }
    },
    {
      $lookup: {
        from: "trabajadors", // Nombre de la colección de pedidos
        localField: "vendedor",
        foreignField: "_id",
        as: "dataVendedores",
        pipeline: [
          {
            $project: {
              nombres: { $concat: ["$nombres", " ", "$apellidos"] }
            }
          }
        ]
      }
    },
    {
      $unwind: "$pedidos"
    },
    {
      $sort: { 'pedidos.createdAt': -1 },
    },
  ];
  if (data.buscador && data.buscador != '') {
    const filtroLimpio = data.buscador.trim(); // Elimina espacios iniciales/finales
    const palabras = filtroLimpio.split(/\s+/); // Divide el texto en palabras por espacios
    const regexBusqueda = palabras.map(palabra => `(?=.*${palabra})`).join('') + '.*'; // Crea el regex: "(?=.*palabra1)(?=.*palabra2).*"
    let texto_busqueda = {
      $match: {
        $or: [
          {
            $expr: {
              $regexMatch: {
                input: { $toString: "$pedidos.id_pedido" }, // Convierte a string
                regex: regexBusqueda,
                options: "i"
              }
            }
          },
          { "pedidos.dataEstablecimiento.nombre_establecimiento": { $regex: regexBusqueda, $options: "i" } },
          { "pedidos.dataEstablecimiento.razon_social": { $regex: regexBusqueda, $options: "i" } },
          { "pedidos.dataEstablecimiento.nit": { $regex: regexBusqueda, $options: "i" } },
          { "pedidos.dataPunto.nombre": { $regex: regexBusqueda, $options: "i" } },
        ]
      }
    };
    consulta.push(texto_busqueda);
  }

  consulta.push(
    {
      $project: {
        _id: "$pedidos._id",
        estadoPedido: "$pedidos.estado",
        valorPedido: "$pedidos.total_pedido",
        metodo_pago: "$pedidos.metodo_pago",
        establecimiento: "$pedidos.dataEstablecimiento.nombre_establecimiento",
        punto: "$pedidos.dataPunto.nombre",
        tipo_persona: "$pedidos.dataEstablecimiento.tipo_usuario",
        nit: "$pedidos.dataEstablecimiento.nit",
        pais: "$pedidos.dataPunto.pais",
        departamento: "$pedidos.dataPunto.departamento",
        ciudad: "$pedidos.dataPunto.ciudad",
        tipo_negocio: "$pedidos.dataEstablecimiento.tipo_negocio",
        fecha_pedido: "$pedidos.createdAt",
        cant_productos: { $size: "$pedidos.productos" },
        puntos_redimidos: "$pedidos.puntos_redimidos",
        codigo_descuento: "$pedidos.codigo_descuento",
        id_pedido: "$pedidos.id_pedido",
        dataVendedores: "$dataVendedores",
        traking: "$pedidos.dataTraking",
        dataCodigos: "$pedidos.dataCodigos",
      }
    },
  )
  this.aggregate(consulta).exec(callback);
};
module.exports.getAllPedidosOnMyVinculationXLS = function (data, callback) {
  const idVendedor = new ObjectId(data.idUser);
  const idDist = new ObjectId(data.idDistribuidor);
  let queryPedido
  if (data.estado === 'curso') {
    queryPedido = [
      { estado: "Pendiente" },
      { estado: "Aprobado Interno" },
      { estado: "Aprobado Externo" },
      { estado: "Alistamiento" },
      { estado: "Despachado" },
    ]
  } else {
    queryPedido = [
      { estado: "Entregado" },
      { estado: "Recibido" },
      { estado: "Calificado" },
      { estado: "Rechazado" },
      { estado: "Cancelado por horeca" },
      { estado: "Cancelado por distribuidor" },
    ]
  }
  let matchQuery
  if (data.tipoUser === 'PROPIETARIO/REP LEGAL' || data.tipoUser === 'ADMINISTRADOR') {
    matchQuery = {
      distribuidor: idDist
    }
  } else {
    matchQuery = {
      vendedor: idVendedor
    }
  }
  this.aggregate([
    {
      $match: matchQuery
    },
    {
      $lookup: {
        from: "pedidos", // Nombre de la colección de pedidos
        localField: "punto_entrega",
        foreignField: "punto_entrega",
        as: "pedidos",
        pipeline: [

          {
            $match: {
              $and: [
                {
                  distribuidor: idDist
                },
                {
                  $or: queryPedido
                }
              ]
            },
          },
          {
            $sort: { createdAt: -1 }
          },
          {

            $lookup: {
              from: "usuario_horecas", // Nombre de la colección de pedidos
              localField: "usuario_horeca",
              foreignField: "_id",
              as: "dataEstablecimiento",
            }
          },
          {

            $lookup: {
              from: "punto_entregas", // Nombre de la colección de pedidos
              localField: "punto_entrega",
              foreignField: "_id",
              as: "dataPunto",
            }
          },
          {

            $lookup: {
              from: "pedido_trackings", // Nombre de la colección de pedidos
              localField: "_id",
              foreignField: "pedido",
              as: "dataTraking",
            }
          },
          {

            $lookup: {
              from: "codigos_descuento_generados", // Nombre de la colección de pedidos
              localField: "codigo_descuento",
              foreignField: "_id",
              as: "dataCodigos",
            }
          },

        ]
      }
    },
    {
      $lookup: {
        from: "trabajadors", // Nombre de la colección de pedidos
        localField: "vendedor",
        foreignField: "_id",
        as: "dataVendedores",
        pipeline: [
          {
            $project: {
              nombres: { $concat: ["$nombres", " ", "$apellidos"] }
            }
          }
        ]
      }
    },
    {
      $unwind: "$pedidos"
    },
    {
      $sort: { 'pedidos.createdAt': -1 },
    },
    {
      $project: {
        _id: "$pedidos._id",
        estadoPedido: "$pedidos.estado",
        valorPedido: "$pedidos.total_pedido",
        metodo_pago: "$pedidos.metodo_pago",
        establecimiento: "$pedidos.dataEstablecimiento.nombre_establecimiento",
        punto: "$pedidos.dataPunto.nombre",
        tipo_persona: "$pedidos.dataEstablecimiento.tipo_usuario",
        nit: "$pedidos.dataEstablecimiento.nit",
        pais: "$pedidos.dataPunto.pais",
        departamento: "$pedidos.dataPunto.departamento",
        ciudad: "$pedidos.dataPunto.ciudad",
        tipo_negocio: "$pedidos.dataEstablecimiento.tipo_negocio",
        fecha_pedido: "$pedidos.createdAt",
        cant_productos: { $size: "$pedidos.productos" },
        puntos_redimidos: "$pedidos.puntos_redimidos",
        codigo_descuento: "$pedidos.codigo_descuento",
        id_pedido: "$pedidos.id_pedido",
        dataVendedores: "$dataVendedores",
        traking: "$pedidos.dataTraking",
        dataCodigos: "$pedidos.dataCodigos",
        // total_documents:"$pedidos.total_documents"
      }
    },
  ]).exec(callback);
};

module.exports.getAllPedidosOnMyVinculationBuscadorCount = function (data, callback) {
  const skip = (data.pagina - 1) * 10;
  const idVendedor = new ObjectId(data.idUser);
  const idDist = new ObjectId(data.idDistribuidor);
  let queryPedido

  queryPedido = [
    { estado: "Aprobado Interno" },
    { estado: "Aprobado Externo" },
    { estado: "Alistamiento" },
    { estado: "Despachado" },
    { estado: "Entregado" },
    { estado: "Recibido" },
    { estado: "Calificado" },
    { estado: "Rechazado" },
    { estado: "Cancelado por horeca" },
    { estado: "Cancelado por distribuidor" },
  ]
  let matchQuery
  if (data.tipoUser === 'PROPIETARIO/REP LEGAL' || data.tipoUser === 'ADMINISTRADOR') {
    matchQuery = {
      distribuidor: idDist,
    }
  } else {
    matchQuery = {
      distribuidor: idDist,
      vendedor: idVendedor
    }
  }
  let consulta = [
    {
      $match: matchQuery
    },
    {
      $lookup: {
        from: "pedidos", // Nombre de la colección de pedidos
        localField: "punto_entrega",
        foreignField: "punto_entrega",
        as: "pedidos",
        pipeline: [

          {
            $match: {
              $and: [
                {
                  distribuidor: idDist
                },
                {
                  $or: queryPedido
                }
              ]
            },
          },
          {
            $sort: { createdAt: -1 }
          },
          {

            $lookup: {
              from: "usuario_horecas", // Nombre de la colección de pedidos
              localField: "usuario_horeca",
              foreignField: "_id",
              as: "dataEstablecimiento",
            }
          },
          {

            $lookup: {
              from: "punto_entregas", // Nombre de la colección de pedidos
              localField: "punto_entrega",
              foreignField: "_id",
              as: "dataPunto",
            }
          },
          {

            $lookup: {
              from: "pedido_trackings", // Nombre de la colección de pedidos
              localField: "_id",
              foreignField: "pedido",
              as: "dataTraking",
            }
          },
          {

            $lookup: {
              from: "codigos_descuento_generados", // Nombre de la colección de pedidos
              localField: "codigo_descuento",
              foreignField: "_id",
              as: "dataCodigos",
            }
          },

        ]
      }
    },
    {
      $lookup: {
        from: "trabajadors", // Nombre de la colección de pedidos
        localField: "vendedor",
        foreignField: "_id",
        as: "dataVendedores",
        pipeline: [
          {
            $project: {
              nombres: { $concat: ["$nombres", " ", "$apellidos"] }
            }
          }
        ]
      }
    },
    {
      $unwind: "$pedidos"
    },
    {
      $sort: { 'pedidos.createdAt': -1 },
    },
  ];
  /*if(data.filtro && data.filtro!= 'all'){
    const filtroLimpio = data.filtro.trim(); // Elimina espacios iniciales/finales
    const palabras = filtroLimpio.split(/\s+/); // Divide el texto en palabras por espacios
    const regexBusqueda = palabras.map(palabra => `(?=.*${palabra})`).join('') + '.*'; // Crea el regex: "(?=.*palabra1)(?=.*palabra2).*"
    let texto_busqueda = {
      $match: { 
        $or: [
          { "pedidos.id_pedido": { $regex: regexBusqueda, $options: "i" } },
          { "pedidos.dataEstablecimiento.nombre_establecimiento": { $regex: regexBusqueda, $options: "i" } },
          { "pedidos.dataEstablecimiento.razon_social": { $regex: regexBusqueda, $options: "i" } },
          { "pedidos.dataEstablecimiento.nit": { $regex: regexBusqueda, $options: "i" } },
          { "pedidos.dataPunto.nombre": { $regex: regexBusqueda, $options: "i" } },
        ]
      }
    };

    consulta.push(texto_busqueda);
  }*/
  if (data.filtro && data.filtro != 'all') {
    const filtroLimpio = data.filtro.trim(); // Elimina espacios iniciales/finales
    const palabras = filtroLimpio.split(/\s+/); // Divide el texto en palabras por espacios
    const regexBusqueda = palabras.map(palabra => `(?=.*${palabra})`).join('') + '.*'; // Crea el regex: "(?=.*palabra1)(?=.*palabra2).*"

    let texto_busqueda = {
      $match: {
        $or: [
          {
            $expr: {
              $regexMatch: {
                input: { $toString: "$pedidos.id_pedido" }, // Convierte a string
                regex: regexBusqueda,
                options: "i"
              }
            }
          },
          { "pedidos.dataEstablecimiento.nombre_establecimiento": { $regex: regexBusqueda, $options: "i" } },
          { "pedidos.dataEstablecimiento.razon_social": { $regex: regexBusqueda, $options: "i" } },
          { "pedidos.dataEstablecimiento.nit": { $regex: regexBusqueda, $options: "i" } },
          { "pedidos.dataPunto.nombre": { $regex: regexBusqueda, $options: "i" } },
        ]
      }
    };

    consulta.push(texto_busqueda);
  }
  consulta.push(
    {
      $skip: skip
    },
    {
      $limit: 10
    },
    {
      $project: {
        _id: "$pedidos._id",
        estadoPedido: "$pedidos.estado",
        valorPedido: "$pedidos.total_pedido",
        metodo_pago: "$pedidos.metodo_pago",
        establecimiento: "$pedidos.dataEstablecimiento.nombre_establecimiento",
        punto: "$pedidos.dataPunto.nombre",
        tipo_persona: "$pedidos.dataEstablecimiento.tipo_usuario",
        nit: "$pedidos.dataEstablecimiento.nit",
        pais: "$pedidos.dataPunto.pais",
        departamento: "$pedidos.dataPunto.departamento",
        ciudad: "$pedidos.dataPunto.ciudad",
        tipo_negocio: "$pedidos.dataEstablecimiento.tipo_negocio",
        fecha_pedido: "$pedidos.createdAt",
        cant_productos: { $size: "$pedidos.productos" },
        puntos_redimidos: "$pedidos.puntos_redimidos",
        codigo_descuento: "$pedidos.codigo_descuento",
        id_pedido: "$pedidos.id_pedido",
        dataVendedores: "$dataVendedores",
        traking: "$pedidos.dataTraking",
        dataCodigos: "$pedidos.dataCodigos",
      }
    },
  )
  this.aggregate(consulta).exec(callback);
};
module.exports.getAllVinculationApp = function (data, callback) {
  data.busqueda;
  this.aggregate([
    {
      $match: data.busqueda
    },
    {
      $lookup: {
        from: "punto_entregas", // Nombre de la colección de pedidos
        localField: "punto_entrega",
        foreignField: "_id",
        as: "dataPunto",
        pipeline: [
          {
            $lookup: {
              from: "usuario_horecas", // Nombre de la colección de pedidos
              localField: "usuario_horeca",
              foreignField: "_id",
              as: "dataEstablecimiento",
            }
          },
        ]
      }
    },
    {
      $lookup: {
        from: "distribuidors", // Nombre de la colección de pedidos
        localField: "distribuidor",
        foreignField: "_id",
        as: "dataDistribuidor",
      }
    },
    {
      $project: {
        id_punto: "$dataPunto._id",
        nombre_punto: "$dataPunto.nombre",
        direccion_punto: "$dataPunto.direccion",
        pais_punto: "$dataPunto.pais",
        telefono: "$dataPunto.telefono",
        pazysalvo: "$pazysalvo",
        informacion_contacto: "$dataPunto.informacion_contacto",
        data_contacto: "$dataPunto.data_contacto",
        ciudad_punto: "$dataPunto.ciudad",
        departamento_punto: "$dataPunto.departamento",
        minimoPedido: "$dataDistribuidor.valor_minimo_pedido",
        tipo_usuario: { $arrayElemAt: ["$dataPunto.dataEstablecimiento.tipo_usuario", 0] },
        logo_establecimiento: { $arrayElemAt: ["$dataPunto.dataEstablecimiento.logo", 0] },
        razon_social: { $arrayElemAt: ["$dataPunto.dataEstablecimiento.razon_social", 0] },
        id_establecimiento: { $arrayElemAt: ["$dataPunto.dataEstablecimiento._id", 0] },
        tipo_negocio_establecimiento: { $arrayElemAt: ["$dataPunto.dataEstablecimiento.tipo_negocio", 0] },
        nombre_establecimiento: { $arrayElemAt: ["$dataPunto.dataEstablecimiento.nombre_establecimiento", 0] },
        //nit: {$arrayElemAt: [ "$dataPunto.dataEstablecimiento.nit", 0 ]},
        nit: {
          $cond: {
            if: { $eq: ["$dataPunto.dataEstablecimiento.tipo_usuario", "Natural"] },
            then: "$dataPunto.dataEstablecimiento.numero_documento",
            else: "$dataPunto.dataEstablecimiento.nit"
          }
        },
        numero_documento: { $arrayElemAt: ["$dataPunto.dataEstablecimiento.numero_documento", 0] },
      }
    },
    {
      $addFields: {
        id_establecimientoSub: { $arrayElemAt: ["$id_establecimiento", 0] },
        nombre_establecimientosub: { $arrayElemAt: ["$nombre_establecimiento", 0] },
        logo_establecimiento: { $arrayElemAt: ["$logo_establecimiento", 0] },
        razon_social: { $arrayElemAt: ["$razon_social", 0] },
        tipo_negocio_establecimiento: { $arrayElemAt: ["$tipo_negocio_establecimiento", 0] },
        nit: { $arrayElemAt: ["$nit", 0] },
        numero_documento: { $arrayElemAt: ["$numero_documento", 0] },
        informacion_contacto: "$informacion_contacto",
        data_contacto: { $arrayElemAt: ["$data_contacto", 0] },
        tipo_usuario: { $arrayElemAt: ["$tipo_usuario", 0] },
      }
    },
    {
      $group: {
        _id: "$id_establecimientoSub",
        nombre_establecimiento: { $last: "$nombre_establecimientosub" },
        tipo_negocio_establecimiento: { $last: "$tipo_negocio_establecimiento" },
        razon_social: { $last: "$razon_social" },
        logo_establecimiento: { $last: "$logo_establecimiento" },
        minimoPedido: { $last: "$minimoPedido" },
        nit: { $last: "$nit" },
        pazysalvo: { $last: "$pazysalvo" },
        tipo_usuario: { $last: "$tipo_usuario" },
        puntos: {
          $push: {
            id_punto: { $arrayElemAt: ["$id_punto", 0] },
            nombre_punto: { $arrayElemAt: ["$nombre_punto", 0] },
            pais: { $arrayElemAt: ["$pais_punto", 0] },
            ciudad: { $arrayElemAt: ["$ciudad_punto", 0] },
            direccion: { $arrayElemAt: ["$direccion_punto", 0] },
            departamento: { $arrayElemAt: ["$departamento_punto", 0] },
            informacion_contacto: { $arrayElemAt: ["$informacion_contacto", 0] },
            //data_contacto: {$arrayElemAt: [ "$data_contacto", 0 ]},
            telefono: { $arrayElemAt: ["$telefono", 0] },
          },
        }
      },
    },
  ]).exec(callback);
};
module.exports.getAllPedidosOnMyVinculationSug = function (data, callback) {
  const skip = (data.pagina - 1) * 10;
  const idVendedor = new ObjectId(data.idUser);
  const idDist = new ObjectId(data.idDistribuidor);
  let filtro
  if (data.tipoUser === 'PROPIETARIO/REP LEGAL' || data.tipoUser === 'ADMINISTRADOR' || data.tipoUser === 'ADMINISTRADOR APROBADOR') {
    filtro = {
      distribuidor: idDist
    }
  } else {
    filtro = {
      vendedor: idVendedor
    }
  }

  this.aggregate([
    {
      $match: filtro
    },
    {
      $lookup: {
        from: "pedidos", // Nombre de la colección de pedidos
        localField: "punto_entrega",
        foreignField: "punto_entrega",
        as: "pedidos",
        pipeline: [
          {
            $match: {
              estado: 'Sugerido'
            },
          },
          {

            $lookup: {
              from: "usuario_horecas", // Nombre de la colección de pedidos
              localField: "usuario_horeca",
              foreignField: "_id",
              as: "dataEstablecimiento",
            }
          },
          {

            $lookup: {
              from: "punto_entregas", // Nombre de la colección de pedidos
              localField: "punto_entrega",
              foreignField: "_id",
              as: "dataPunto",
            }
          },
          {

            $lookup: {
              from: "pedido_trackings", // Nombre de la colección de pedidos
              localField: "_id",
              foreignField: "pedido",
              as: "dataTraking",
            }
          },
          {

            $lookup: {
              from: "codigos_descuento_generados", // Nombre de la colección de pedidos
              localField: "codigo_descuento",
              foreignField: "_id",
              as: "dataCodigos",
            }
          },


        ]
      }
    },

    {
      $lookup: {
        from: "trabajadors", // Nombre de la colección de pedidos
        localField: "vendedor",
        foreignField: "_id",
        as: "dataVendedores",
        pipeline: [
          {
            $project: {
              nombres: { $concat: ["$nombres", " ", "$apellidos"] }
            }
          }
        ]
      }
    },
    {
      $unwind: "$pedidos"
    },
    {
      $sort: { "pedidos.createdAt": -1 }
    },
    {
      $skip: skip
    },
    {
      $limit: 10
    },
    {
      $project: {
        _id: "$pedidos._id",
        estadoPedido: "$pedidos.estado",
        valorPedido: "$pedidos.total_pedido",
        metodo_pago: "$pedidos.metodo_pago",
        establecimiento: "$pedidos.dataEstablecimiento.nombre_establecimiento",
        punto: "$pedidos.dataPunto.nombre",
        tipo_persona: "$pedidos.dataEstablecimiento.tipo_usuario",
        nit: "$pedidos.dataEstablecimiento.nit",
        pais: "$pedidos.dataPunto.pais",
        departamento: "$pedidos.dataPunto.departamento",
        ciudad: "$pedidos.dataPunto.ciudad",
        tipo_negocio: "$pedidos.dataEstablecimiento.tipo_negocio",
        fecha_pedido: "$pedidos.createdAt",
        cant_productos: { $size: "$pedidos.productos" },
        puntos_redimidos: "$pedidos.puntos_redimidos",
        codigo_descuento: "$pedidos.codigo_descuento",
        id_pedido: "$pedidos.id_pedido",
        dataVendedores: "$dataVendedores",
        traking: "$pedidos.dataTraking",
        dataCodigos: "$pedidos.dataCodigos"
      }
    },


  ]).exec(callback);
};
module.exports.getAllPedidosOnMyVinculationSugCloneCount = function (data, callback) {
  const idVendedor = new ObjectId(data.idUser);
  const idDist = new ObjectId(data.idDistribuidor);
  let filtro
  if (data.tipoUser === 'PROPIETARIO/REP LEGAL' || data.tipoUser === 'ADMINISTRADOR' || data.tipoUser === 'ADMINISTRADOR APROBADOR') {
    filtro = {
      distribuidor: idDist
    }
  } else {
    filtro = {
      vendedor: idVendedor
    }
  }

  this.aggregate([
    {
      $match: filtro
    },
    {
      $lookup: {
        from: "pedidos", // Nombre de la colección de pedidos
        localField: "punto_entrega",
        foreignField: "punto_entrega",
        as: "pedidos",
        pipeline: [
          {
            $match: { estado: 'Sugerido' },
          },
          {
            $sort: { createdAt: -1 }
          },
          {

            $lookup: {
              from: "usuario_horecas", // Nombre de la colección de pedidos
              localField: "usuario_horeca",
              foreignField: "_id",
              as: "dataEstablecimiento",
            }
          },
          {

            $lookup: {
              from: "punto_entregas", // Nombre de la colección de pedidos
              localField: "punto_entrega",
              foreignField: "_id",
              as: "dataPunto",
            }
          },
          {

            $lookup: {
              from: "pedido_trackings", // Nombre de la colección de pedidos
              localField: "_id",
              foreignField: "pedido",
              as: "dataTraking",
            }
          },
          {

            $lookup: {
              from: "codigos_descuento_generados", // Nombre de la colección de pedidos
              localField: "codigo_descuento",
              foreignField: "_id",
              as: "dataCodigos",
            }
          },


        ]
      }
    },
    {
      $lookup: {
        from: "trabajadors", // Nombre de la colección de pedidos
        localField: "vendedor",
        foreignField: "_id",
        as: "dataVendedores",
        pipeline: [
          {
            $project: {
              nombres: { $concat: ["$nombres", " ", "$apellidos"] }
            }
          }
        ]
      }
    },
    {
      $unwind: "$pedidos"
    },
    {
      $project: {
        _id: "$pedidos._id",
        estadoPedido: "$pedidos.estado",
        valorPedido: "$pedidos.total_pedido",
        metodo_pago: "$pedidos.metodo_pago",
        establecimiento: "$pedidos.dataEstablecimiento.nombre_establecimiento",
        punto: "$pedidos.dataPunto.nombre",
        tipo_persona: "$pedidos.dataEstablecimiento.tipo_usuario",
        nit: "$pedidos.dataEstablecimiento.nit",
        pais: "$pedidos.dataPunto.pais",
        departamento: "$pedidos.dataPunto.departamento",
        ciudad: "$pedidos.dataPunto.ciudad",
        tipo_negocio: "$pedidos.dataEstablecimiento.tipo_negocio",
        fecha_pedido: "$pedidos.createdAt",
        cant_productos: { $size: "$pedidos.productos" },
        puntos_redimidos: "$pedidos.puntos_redimidos",
        codigo_descuento: "$pedidos.codigo_descuento",
        id_pedido: "$pedidos.id_pedido",
        dataVendedores: "$dataVendedores",
        traking: "$pedidos.dataTraking",
        dataCodigos: "$pedidos.dataCodigos"
      }
    }
  ]).exec(callback);
};
module.exports.getDisVinculacionHorecaAll = function (data, callback) {
  this.aggregate([
    {
      $match: data,
    },
    {
      $lookup: {
        from: Puntos_entregaBD, // Nombre de la colección a relacionar
        localField: "punto_entrega", // Campo de la colección actual
        foreignField: "_id", // Campo de la colección a relacionar
        as: "punto_entrega", // Nombre del campo donde se insertarán los documentos relacionados
        pipeline: [
          {
            $lookup: {
              from: "usuario_horecas", // Nombre de la colección a relacionar
              localField: "usuario_horeca", // Campo de la colección actual
              foreignField: "_id", // Campo de la colección a relacionar
              as: "usuario_horeca", // Nombre del campo donde se insertarán los documentos relacionados
            },
          },
        ],
      },
    },
    {
      $project: {
        _id: {
          $arrayElemAt: ["$punto_entrega._id", 0],
        },
        nombre: {
          $arrayElemAt: ["$punto_entrega.nombre", 0],
        },
        direccion: {
          $arrayElemAt: ["$punto_entrega.direccion", 0],
        },
        ciudad: {
          $arrayElemAt: ["$punto_entrega.ciudad", 0],
        },
        departamento: {
          $arrayElemAt: ["$punto_entrega.departamento", 0],
        },
        nit: {
          $arrayElemAt: [
            { $arrayElemAt: ["$punto_entrega.usuario_horeca.nit", 0] }, 0
          ]
        }
      },
    },
  ]).exec(callback);
};
module.exports.getRegistro = function (query, callback) {
  Distribuidores_vinculados.findOne(query)
    .populate({
      path: "punto_entrega",
      populate: {
        path: "usuario_horeca",
      },
    })
    .populate({
      path: "distribuidor",
    })
    .exec(callback);
};
module.exports.getDisVinculacionHorecaNuevos = function (data, callback) {
  const threeMonthsAgo = moment().subtract(3, 'months').toDate();
  const today = new Date();
  this.aggregate([
    {
      $match: {
        distribuidor: new ObjectId(data.distribuidor),
      },
    },
    {
      $limit: 30
    },
    {
      $sort: { createdAt: -1 }
    },
    {
      $lookup: {
        from: "reportepedidos", //Nombre de la colecccion a relacionar
        localField: "punto_entrega", //Nombre del campo de la coleccion actual
        foreignField: "idPunto", //Nombre del campo de la coleccion a relacionar
        as: "dataPedidos3Meses", //Nombre del campo donde se insertara todos los documentos relacionados
        pipeline: [
          {
            $match: {
              idDistribuidor: new ObjectId(data.distribuidor),
              createdAt: { $gte: threeMonthsAgo, $lt: today },
              estaActTraking: { $in: ['Entregado', 'Recibido', 'Calificado'] }

            }
          },
          {
            $facet: {
              totalCompra: [
                {
                  $group: {
                    _id: "$idPedido",
                    total: { $last: "$subtotalCompra" },
                  },
                },
                {
                  $addFields: {
                    bandera: "pedido"
                  }
                },
                {
                  $group: {
                    _id: "$bandera",
                    count: { $sum: "$total" },
                  },
                },
              ],
              totalPedidos: [
                {
                  $group: {
                    _id: "$idPedido",
                    count: { $sum: 1 },
                  },
                },
                {
                  $addFields: {
                    bandera: "pedido"
                  }
                },
                {
                  $group: {
                    _id: "$bandera",
                    count: { $sum: 1 },
                  },
                },



              ],
              totalProductos: [
                {
                  $group: {
                    _id: "$codigoFeatProducto",
                    count: { $sum: 1 },
                  },
                },
                {
                  $addFields: {
                    bandera: "producto"
                  }
                },
                {
                  $group: {
                    _id: "$bandera",
                    count: { $sum: 1 },
                  },
                },
              ],
            },
          },
        ]
      },
    },
    {
      $lookup: {
        from: Puntos_entregaBD, //Nombre de la colecccion a relacionar
        localField: "punto_entrega", //Nombre del campo de la coleccion actual
        foreignField: "_id", //Nombre del campo de la coleccion a relacionar
        as: "punto_entrega", //Nombre del campo donde se insertara todos los documentos relacionados
        pipeline: [
          {
            $lookup: {
              from: "usuario_horecas", //Nombre de la colecccion a relacionar
              localField: "usuario_horeca", //Nombre del campo de la coleccion actual
              foreignField: "_id", //Nombre del campo de la coleccion a relacionar
              as: "dataHoreca", //Nombre del campo donde se insertara todos los documentos relacionados
              pipeline: [
                {
                  $project: {
                    tipoNegocio: "$tipo_negocio",
                    tipoPersona: "$tipo_usuario",
                    nit: "$nit",
                    razon_social: "$razon_social",
                    nombre: "$nombre_establecimiento",
                    propietario_correo: "$propietario_correo",
                    propietario_telefono: "$propietario_telefono",

                    id: "$_id"
                  }
                },
                {
                  $lookup: {
                    from: 'trabajadors', //Nombre de la colecccion a relacionar
                    localField: "_id", //Nombre del campo de la coleccion actual
                    foreignField: "usuario_horeca", //Nombre del campo de la coleccion a relacionar
                    as: "dataTrabajadorPunto", //Nombre del campo donde se insertara todos los documentos relacionados
                  }
                },
              ]
            },
          }, {
            $addFields: {
              nombreEncargado: { $arrayElemAt: ["$informacion_contacto", 0] },
              apellidoEncargado: { $arrayElemAt: ["$informacion_contacto", 1] },
              emailEncargado: { $arrayElemAt: ["$informacion_contacto", 2] },
              telefonoEncargado: { $arrayElemAt: ["$informacion_contacto", 3] },

            }
          }
        ]
      },
    },
    {
      $lookup: {
        from: "trabajadors", //Nombre de la colecccion a relacionar
        localField: "vendedor", //Nombre del campo de la coleccion actual
        foreignField: "_id", //Nombre del campo de la coleccion a relacionar
        as: "equipo_comercial", //Nombre del campo donde se insertara todos los documentos relacionados
        pipeline: [
          {
            $project: {
              nombre: { $concat: ["$nombres", " ", "$apellidos"] },
              tipo_trabajador: "$tipo_trabajador"
            }
          }
        ]
      },
    },
    {
      $project: {
        _id: "$_id",
        dataPedidos3Meses: "$dataPedidos3Meses",
        estado_vinculacion: "$estado",
        tipo_negocio: { $last: "$punto_entrega.dataHoreca.tipoNegocio" },
        nombre: { $last: "$punto_entrega.dataHoreca.nombre" },
        razon_social: { $last: "$punto_entrega.dataHoreca.razon_social" },
        tipo_persona: { $last: "$punto_entrega.dataHoreca.tipoPersona" },
        nit: { $last: "$punto_entrega.dataHoreca.nit" },
        pais: { $last: "$punto_entrega.pais" },
        ciudad: { $last: "$punto_entrega.ciudad" },
        departamento: { $last: "$punto_entrega.departamento" },
        punto_entrega: { $last: "$punto_entrega.nombre" },
        direccion: { $last: "$punto_entrega.direccion" },
        sillas: { $last: "$punto_entrega.sillas" },
        domicilio: { $last: "$punto_entrega.domicilios" },
        precio_especial: "$convenio",
        estadoCartera: "$pazysalvo",
        equipo: "$equipo_comercial",

        emailPropietario: { $last: "$punto_entrega.dataHoreca.propietario_correo" },
        telPropietario: { $last: "$punto_entrega.dataHoreca.propietario_telefono" },

        dataHoreca: { $last: "$punto_entrega.dataHoreca" },
        emailEncargado: { $last: "$punto_entrega.emailEncargado" },
        telefonoEncargado: { $last: "$punto_entrega.telefonoEncargado" },
        nombreEncargado: { $concat: [{ $last: "$punto_entrega.nombreEncargado" }, " ", { $last: "$punto_entrega.apellidoEncargado" }] },
        totalPedidos3MesesCompra: { $last: "$dataPedidos3Meses.totalCompra.count" },
        totalProductos3Meses: { $last: "$dataPedidos3Meses.totalProductos.count" },
        totalPedidos3Meses: { $last: "$dataPedidos3Meses.totalPedidos.count" },
        dataTrabajadorPunto: { $arrayElemAt: ["$punto_entrega.dataHoreca.dataTrabajadorPunto", 0] },
      }
    }
  ]).exec(callback);
};
module.exports.getDisVinculacionHorecaCreados = function (data, callback) {
  const skip = (data.pagina - 1) * 10;
  const totalPaginas = Math.ceil(data.totalVinculaciones / 10);
  const threeMonthsAgo = moment().subtract(3, 'months').toDate();
  const today = new Date();
  this.aggregate([
    {
      $match: {
        distribuidor: new ObjectId(data.distribuidor),
        creado_distribuidor: true,
      },
    },
    {
      $skip: skip
    },
    {
      $limit: 10
    },
    {
      $lookup: {
        from: "reportepedidos", //Nombre de la colecccion a relacionar
        localField: "punto_entrega", //Nombre del campo de la coleccion actual
        foreignField: "idPunto", //Nombre del campo de la coleccion a relacionar
        as: "dataPedidos3Meses", //Nombre del campo donde se insertara todos los documentos relacionados
        pipeline: [
          {
            $match: {
              idDistribuidor: new ObjectId(data.distribuidor),
              createdAt: { $gte: threeMonthsAgo, $lt: today },
              estaActTraking: { $in: ['Entregado', 'Recibido', 'Calificado'] }

            }
          },
          {
            $facet: {
              totalCompra: [
                {
                  $group: {
                    _id: "$idPedido",
                    total: { $last: "$subtotalCompra" },
                  },
                },
                {
                  $addFields: {
                    bandera: "pedido"
                  }
                },
                {
                  $group: {
                    _id: "$bandera",
                    count: { $sum: "$total" },
                  },
                },
              ],
              totalPedidos: [
                {
                  $group: {
                    _id: "$idPedido",
                    count: { $sum: 1 },
                  },
                },
                {
                  $addFields: {
                    bandera: "pedido"
                  }
                },
                {
                  $group: {
                    _id: "$bandera",
                    count: { $sum: 1 },
                  },
                },



              ],
              totalProductos: [
                {
                  $group: {
                    _id: "$codigoFeatProducto",
                    count: { $sum: 1 },
                  },
                },
                {
                  $addFields: {
                    bandera: "producto"
                  }
                },
                {
                  $group: {
                    _id: "$bandera",
                    count: { $sum: 1 },
                  },
                },
              ],
            },
          },
        ]
      },
    },
    {
      $lookup: {
        from: Puntos_entregaBD, //Nombre de la colecccion a relacionar
        localField: "punto_entrega", //Nombre del campo de la coleccion actual
        foreignField: "_id", //Nombre del campo de la coleccion a relacionar
        as: "punto_entrega", //Nombre del campo donde se insertara todos los documentos relacionados
        pipeline: [
          {
            $lookup: {
              from: "usuario_horecas", //Nombre de la colecccion a relacionar
              localField: "usuario_horeca", //Nombre del campo de la coleccion actual
              foreignField: "_id", //Nombre del campo de la coleccion a relacionar
              as: "dataHoreca", //Nombre del campo donde se insertara todos los documentos relacionados
              pipeline: [
                {
                  $project: {
                    tipoNegocio: "$tipo_negocio",
                    tipoPersona: "$tipo_usuario",
                    nit: "$nit",
                    razon_social: "$razon_social",
                    nombre: "$nombre_establecimiento",
                    propietario_correo: "$propietario_correo",
                    propietario_telefono: "$propietario_telefono",

                    id: "$_id"
                  }
                },
                {
                  $lookup: {
                    from: 'trabajadors', //Nombre de la colecccion a relacionar
                    localField: "_id", //Nombre del campo de la coleccion actual
                    foreignField: "usuario_horeca", //Nombre del campo de la coleccion a relacionar
                    as: "dataTrabajadorPunto", //Nombre del campo donde se insertara todos los documentos relacionados
                  }
                },
              ]
            },
          }, {
            $addFields: {
              nombreEncargado: { $arrayElemAt: ["$informacion_contacto", 0] },
              apellidoEncargado: { $arrayElemAt: ["$informacion_contacto", 1] },
              emailEncargado: { $arrayElemAt: ["$informacion_contacto", 2] },
              telefonoEncargado: { $arrayElemAt: ["$informacion_contacto", 3] },

            }
          }
        ]
      },
    },
    {
      $lookup: {
        from: "trabajadors", //Nombre de la colecccion a relacionar
        localField: "vendedor", //Nombre del campo de la coleccion actual
        foreignField: "_id", //Nombre del campo de la coleccion a relacionar
        as: "equipo_comercial", //Nombre del campo donde se insertara todos los documentos relacionados
        pipeline: [
          {
            $project: {
              nombre: { $concat: ["$nombres", " ", "$apellidos"] },
              tipo_trabajador: "$tipo_trabajador"
            }
          }
        ]
      },
    },
    {
      $project: {
        _id: "$_id",
        dataPedidos3Meses: "$dataPedidos3Meses",
        estado_vinculacion: "$estado",
        tipo_negocio: { $last: "$punto_entrega.dataHoreca.tipoNegocio" },
        nombre: { $last: "$punto_entrega.dataHoreca.nombre" },
        razon_social: { $last: "$punto_entrega.dataHoreca.razon_social" },
        tipo_persona: { $last: "$punto_entrega.dataHoreca.tipoPersona" },
        nit: { $last: "$punto_entrega.dataHoreca.nit" },
        pais: { $last: "$punto_entrega.pais" },
        ciudad: { $last: "$punto_entrega.ciudad" },
        departamento: { $last: "$punto_entrega.departamento" },
        punto_entrega: { $last: "$punto_entrega.nombre" },
        direccion: { $last: "$punto_entrega.direccion" },
        sillas: { $last: "$punto_entrega.sillas" },
        domicilio: { $last: "$punto_entrega.domicilios" },
        precio_especial: "$convenio",
        estadoCartera: "$pazysalvo",
        equipo: "$equipo_comercial",

        emailPropietario: { $last: "$punto_entrega.dataHoreca.propietario_correo" },
        telPropietario: { $last: "$punto_entrega.dataHoreca.propietario_telefono" },

        dataHoreca: { $last: "$punto_entrega.dataHoreca" },
        emailEncargado: { $last: "$punto_entrega.emailEncargado" },
        telefonoEncargado: { $last: "$punto_entrega.telefonoEncargado" },
        nombreEncargado: { $concat: [{ $last: "$punto_entrega.nombreEncargado" }, " ", { $last: "$punto_entrega.apellidoEncargado" }] },
        totalPedidos3MesesCompra: { $last: "$dataPedidos3Meses.totalCompra.count" },
        totalProductos3Meses: { $last: "$dataPedidos3Meses.totalProductos.count" },
        totalPedidos3Meses: { $last: "$dataPedidos3Meses.totalPedidos.count" },
        dataTrabajadorPunto: { $arrayElemAt: ["$punto_entrega.dataHoreca.dataTrabajadorPunto", 0] },
      }
    }
  ]).exec(callback);
};
module.exports.getDisVinculacionHorecaActivos = function (data, callback) {
  const skip = (data.pagina - 1) * 10;
  const threeMonthsAgo = moment().subtract(3, 'months').toDate();
  const today = new Date();

  // Construir el objeto $match dinámicamente
  const match = {
    distribuidor: new ObjectId(data.distribuidor),
  };

  if (data.id_trabajador) {
    match.vendedor = new ObjectId(data.id_trabajador);
  }

  const pipeline = [
    { $match: match },
    {
      $lookup: {
        from: "reportepedidos",
        localField: "punto_entrega",
        foreignField: "idPunto",
        as: "dataPedidos3Meses",
        pipeline: [
          {
            $match: {
              idDistribuidor: new ObjectId(data.distribuidor),
              createdAt: { $gte: threeMonthsAgo, $lt: today },
            },
          },
          {
            $facet: {
              totalCompra: [
                {
                  $group: {
                    _id: "$idPedido",
                    total: { $last: "$subtotalCompra" },
                  },
                },
                {
                  $addFields: { bandera: "pedido" },
                },
                {
                  $group: {
                    _id: "$bandera",
                    count: { $sum: "$total" },
                  },
                },
              ],
              totalPedidos: [
                {
                  $group: {
                    _id: "$idPedido",
                    count: { $sum: 1 },
                  },
                },
                {
                  $addFields: { bandera: "pedido" },
                },
                {
                  $group: {
                    _id: "$bandera",
                    count: { $sum: 1 },
                  },
                },
              ],
              totalProductos: [
                {
                  $group: {
                    _id: "$codigoFeatProducto",
                    count: { $sum: 1 },
                  },
                },
                {
                  $addFields: { bandera: "producto" },
                },
                {
                  $group: {
                    _id: "$bandera",
                    count: { $sum: 1 },
                  },
                },
              ],
            },
          },
          {
            $match: {
              "totalCompra": { $exists: true, $ne: [] },
            },
          },
        ],
      },
    },
    {
      $match: {
        "dataPedidos3Meses": { $ne: [] },
      },
    },
    {
      $lookup: {
        from: Puntos_entregaBD,
        localField: "punto_entrega",
        foreignField: "_id",
        as: "punto_entrega",
        pipeline: [
          {
            $lookup: {
              from: "usuario_horecas",
              localField: "usuario_horeca",
              foreignField: "_id",
              as: "dataHoreca",
              pipeline: [
                {
                  $project: {
                    tipoNegocio: "$tipo_negocio",
                    tipoPersona: "$tipo_usuario",
                    nit: "$nit",
                    razon_social: "$razon_social",
                    nombre: "$nombre_establecimiento",
                    propietario_correo: "$propietario_correo",
                    propietario_telefono: "$propietario_telefono",
                    id: "$_id"
                  }
                },
                {
                  $lookup: {
                    from: 'trabajadors',
                    localField: "_id",
                    foreignField: "usuario_horeca",
                    as: "dataTrabajadorPunto",
                  }
                },
              ]
            },
          },
          {
            $addFields: {
              nombreEncargado: { $arrayElemAt: ["$informacion_contacto", 0] },
              apellidoEncargado: { $arrayElemAt: ["$informacion_contacto", 1] },
              emailEncargado: { $arrayElemAt: ["$informacion_contacto", 2] },
              telefonoEncargado: { $arrayElemAt: ["$informacion_contacto", 3] },
            }
          }
        ]
      },
    },
    {
      $lookup: {
        from: "trabajadors",
        localField: "vendedor",
        foreignField: "_id",
        as: "equipo_comercial",
        pipeline: [
          {
            $project: {
              nombre: { $concat: ["$nombres", " ", "$apellidos"] },
              tipo_trabajador: "$tipo_trabajador"
            }
          }
        ]
      },
    },
  ];

  if (data.busqueda) {
    pipeline.push({
      $match: {
        $or: [
          { "punto_entrega.dataHoreca.nombre": { $regex: data.busqueda, $options: 'i' } },
          { "punto_entrega.dataHoreca.razon_social": { $regex: data.busqueda, $options: 'i' } },
          { "punto_entrega.dataHoreca.nit": { $regex: data.busqueda, $options: 'i' } }
        ]
      }
    });
  }

  if (data.tipo_negocio) {
    pipeline.push({
      $match: {
        "punto_entrega.dataHoreca.tipo_negocio": data.tipo_negocio
      }
    });
  }
  pipeline.push({
    $project: {
      _id: "$_id",
      dataPedidos3Meses: "$dataPedidos3Meses",
      estado_vinculacion: "$estado",
      tipo_negocio: { $last: "$punto_entrega.dataHoreca.tipoNegocio" },
      nombre: { $last: "$punto_entrega.dataHoreca.nombre" },
      razon_social: { $last: "$punto_entrega.dataHoreca.razon_social" },
      tipo_persona: { $last: "$punto_entrega.dataHoreca.tipoPersona" },
      nit: { $last: "$punto_entrega.dataHoreca.nit" },
      pais: { $last: "$punto_entrega.pais" },
      ciudad: { $last: "$punto_entrega.ciudad" },
      departamento: { $last: "$punto_entrega.departamento" },
      punto_entrega: { $last: "$punto_entrega.nombre" },
      direccion: { $last: "$punto_entrega.direccion" },
      sillas: { $last: "$punto_entrega.sillas" },
      domicilio: { $last: "$punto_entrega.domicilios" },
      precio_especial: "$convenio",
      estadoCartera: "$pazysalvo",
      equipo: "$equipo_comercial",
      emailPropietario: { $last: "$punto_entrega.dataHoreca.propietario_correo" },
      telPropietario: { $last: "$punto_entrega.dataHoreca.propietario_telefono" },
      dataHoreca: { $last: "$punto_entrega.dataHoreca" },
      emailEncargado: { $last: "$punto_entrega.emailEncargado" },
      telefonoEncargado: { $last: "$punto_entrega.telefonoEncargado" },
      nombreEncargado: { $concat: [{ $last: "$punto_entrega.nombreEncargado" }, " ", { $last: "$punto_entrega.apellidoEncargado" }] },
      totalPedidos3MesesCompra: { $last: "$dataPedidos3Meses.totalCompra.count" },
      totalProductos3Meses: { $last: "$dataPedidos3Meses.totalProductos.count" },
      totalPedidos3Meses: { $last: "$dataPedidos3Meses.totalPedidos.count" },
      dataTrabajadorPunto: { $arrayElemAt: ["$punto_entrega.dataHoreca.dataTrabajadorPunto", 0] },
    }
  });


  pipeline.push({ $skip: skip });
  pipeline.push({ $limit: 10 });

  this.aggregate(pipeline).exec(callback);
};
module.exports.getAllPedidosOnMyVinculation_buscador = function (data, callback) {
  const skip = (data.pagina - 1) * 10;
  const idVendedor = new ObjectId(data.trabajador);
  const idDist = new ObjectId(data.idDistribuidor);
  let queryPedido

  queryPedido = [
    { estado: "Aprobado Interno" },
    { estado: "Aprobado Externo" },
    { estado: "Alistamiento" },
    { estado: "Despachado" },
    { estado: "Entregado" },
    { estado: "Recibido" },
    { estado: "Calificado" },
    { estado: "Rechazado" },
    { estado: "Cancelado por horeca" },
    { estado: "Cancelado por distribuidor" },
  ]
  let matchQuery
  if (!data.trabajador && (data.tipoUser === 'PROPIETARIO/REP LEGAL' || data.tipoUser === 'ADMINISTRADOR')) {
    matchQuery = {
      distribuidor: idDist,
    }
  } else {
    matchQuery = {
      vendedor: idVendedor,
      distribuidor: idDist,
    }
  }
  let consulta = [
    {
      $match: matchQuery
    },
    {
      $lookup: {
        from: "pedidos", // Nombre de la colección de pedidos
        localField: "punto_entrega",
        foreignField: "punto_entrega",
        as: "pedidos",
        pipeline: [

          {
            $match: {
              $or: queryPedido
            },
          },
          {
            $sort: { createdAt: -1 }
          },
          {

            $lookup: {
              from: "usuario_horecas", // Nombre de la colección de pedidos
              localField: "usuario_horeca",
              foreignField: "_id",
              as: "dataEstablecimiento",
            }
          },
          {

            $lookup: {
              from: "punto_entregas", // Nombre de la colección de pedidos
              localField: "punto_entrega",
              foreignField: "_id",
              as: "dataPunto",
            }
          },
          {

            $lookup: {
              from: "pedido_trackings", // Nombre de la colección de pedidos
              localField: "_id",
              foreignField: "pedido",
              as: "dataTraking",
            }
          },
          {

            $lookup: {
              from: "codigos_descuento_generados", // Nombre de la colección de pedidos
              localField: "codigo_descuento",
              foreignField: "_id",
              as: "dataCodigos",
            }
          },

        ]
      }
    },
    {
      $lookup: {
        from: "trabajadors", // Nombre de la colección de pedidos
        localField: "vendedor",
        foreignField: "_id",
        as: "dataVendedores",
        pipeline: [
          {
            $project: {
              nombres: { $concat: ["$nombres", " ", "$apellidos"] }
            }
          }
        ]
      }
    },
    {
      $unwind: "$pedidos"
    },
    {
      $sort: { 'pedidos.createdAt': -1 },
    },
  ];

  if (data.filtro && data.filtro != 'all') {
    const filtroLimpio = data.filtro.trim(); // Elimina espacios iniciales/finales
    const palabras = filtroLimpio.split(/\s+/); // Divide el texto en palabras por espacios
    const regexBusqueda = palabras.map(palabra => `(?=.*${palabra})`).join('') + '.*'; // Crea el regex: "(?=.*palabra1)(?=.*palabra2).*"

    let texto_busqueda = {
      $match: {
        $or: [
          {
            $expr: {
              $regexMatch: {
                input: { $toString: "$pedidos.id_pedido" }, // Convierte a string
                regex: regexBusqueda,
                options: "i"
              }
            }
          },
          { "pedidos.dataEstablecimiento.nombre_establecimiento": { $regex: regexBusqueda, $options: "i" } },
          { "pedidos.dataEstablecimiento.razon_social": { $regex: regexBusqueda, $options: "i" } },
          { "pedidos.dataEstablecimiento.nit": { $regex: regexBusqueda, $options: "i" } },
          { "pedidos.dataPunto.nombre": { $regex: regexBusqueda, $options: "i" } },
        ]
      }
    };

    consulta.push(texto_busqueda);
  }
  consulta.push(
    {
      $skip: skip
    },
    {
      $limit: 10
    },
    {
      $project: {
        _id: "$pedidos._id",
        estadoPedido: "$pedidos.estado",
        valorPedido: "$pedidos.total_pedido",
        metodo_pago: "$pedidos.metodo_pago",
        establecimiento: "$pedidos.dataEstablecimiento.nombre_establecimiento",
        punto: "$pedidos.dataPunto.nombre",
        tipo_persona: "$pedidos.dataEstablecimiento.tipo_usuario",
        nit: "$pedidos.dataEstablecimiento.nit",
        pais: "$pedidos.dataPunto.pais",
        departamento: "$pedidos.dataPunto.departamento",
        ciudad: "$pedidos.dataPunto.ciudad",
        tipo_negocio: "$pedidos.dataEstablecimiento.tipo_negocio",
        fecha_pedido: "$pedidos.createdAt",
        cant_productos: { $size: "$pedidos.productos" },
        puntos_redimidos: "$pedidos.puntos_redimidos",
        codigo_descuento: "$pedidos.codigo_descuento",
        id_pedido: "$pedidos.id_pedido",
        dataVendedores: "$dataVendedores",
        traking: "$pedidos.dataTraking",
        dataCodigos: "$pedidos.dataCodigos",
      }
    },
  )
  this.aggregate(consulta).exec(callback);
};
module.exports.getAllPedidosOnMyVinculation_buscadorCloneCount = function (data, callback) {
  const idVendedor = new ObjectId(data.trabajador);
  const idDist = new ObjectId(data.idDistribuidor);
  let queryPedido

  queryPedido = [
    { estado: "Aprobado Interno" },
    { estado: "Aprobado Externo" },
    { estado: "Alistamiento" },
    { estado: "Despachado" },
    { estado: "Entregado" },
    { estado: "Recibido" },
    { estado: "Calificado" },
    { estado: "Rechazado" },
    { estado: "Cancelado por horeca" },
    { estado: "Cancelado por distribuidor" },
  ]
  let matchQuery
  if (!data.trabajador && (data.tipoUser === 'PROPIETARIO/REP LEGAL' || data.tipoUser === 'ADMINISTRADOR')) {
    matchQuery = {
      distribuidor: idDist,
    }
  } else {
    matchQuery = {
      vendedor: idVendedor,
      distribuidor: idDist,
    }
  }
  let consulta = [
    {
      $match: matchQuery
    },
    {
      $lookup: {
        from: "pedidos", // Nombre de la colección de pedidos
        localField: "punto_entrega",
        foreignField: "punto_entrega",
        as: "pedidos",
        pipeline: [

          {
            $match: {
              $or: queryPedido
            },
          },
          {
            $sort: { createdAt: -1 }
          },
          {

            $lookup: {
              from: "usuario_horecas", // Nombre de la colección de pedidos
              localField: "usuario_horeca",
              foreignField: "_id",
              as: "dataEstablecimiento",
            }
          },
          {

            $lookup: {
              from: "punto_entregas", // Nombre de la colección de pedidos
              localField: "punto_entrega",
              foreignField: "_id",
              as: "dataPunto",
            }
          },
          {

            $lookup: {
              from: "pedido_trackings", // Nombre de la colección de pedidos
              localField: "_id",
              foreignField: "pedido",
              as: "dataTraking",
            }
          },
          {

            $lookup: {
              from: "codigos_descuento_generados", // Nombre de la colección de pedidos
              localField: "codigo_descuento",
              foreignField: "_id",
              as: "dataCodigos",
            }
          },

        ]
      }
    },
    {
      $lookup: {
        from: "trabajadors", // Nombre de la colección de pedidos
        localField: "vendedor",
        foreignField: "_id",
        as: "dataVendedores",
        pipeline: [
          {
            $project: {
              nombres: { $concat: ["$nombres", " ", "$apellidos"] }
            }
          }
        ]
      }
    },
    {
      $unwind: "$pedidos"
    },
    {
      $sort: { 'pedidos.createdAt': -1 },
    },
  ];
  if (data.filtro && data.filtro != 'all') {
    const filtroLimpio = data.filtro.trim(); // Elimina espacios iniciales/finales
    const palabras = filtroLimpio.split(/\s+/); // Divide el texto en palabras por espacios
    const regexBusqueda = palabras.map(palabra => `(?=.*${palabra})`).join('') + '.*'; // Crea el regex: "(?=.*palabra1)(?=.*palabra2).*"

    let texto_busqueda = {
      $match: {
        $or: [
          {
            $expr: {
              $regexMatch: {
                input: { $toString: "$pedidos.id_pedido" }, // Convierte a string
                regex: regexBusqueda,
                options: "i"
              }
            }
          },
          { "pedidos.dataEstablecimiento.nombre_establecimiento": { $regex: regexBusqueda, $options: "i" } },
          { "pedidos.dataEstablecimiento.razon_social": { $regex: regexBusqueda, $options: "i" } },
          { "pedidos.dataEstablecimiento.nit": { $regex: regexBusqueda, $options: "i" } },
          { "pedidos.dataPunto.nombre": { $regex: regexBusqueda, $options: "i" } },
        ]
      }
    };

    consulta.push(texto_busqueda);
  }
  consulta.push(
    {
      $project: {
        _id: "$pedidos._id",
        estadoPedido: "$pedidos.estado",
        valorPedido: "$pedidos.total_pedido",
        metodo_pago: "$pedidos.metodo_pago",
        establecimiento: "$pedidos.dataEstablecimiento.nombre_establecimiento",
        punto: "$pedidos.dataPunto.nombre",
        tipo_persona: "$pedidos.dataEstablecimiento.tipo_usuario",
        nit: "$pedidos.dataEstablecimiento.nit",
        pais: "$pedidos.dataPunto.pais",
        departamento: "$pedidos.dataPunto.departamento",
        ciudad: "$pedidos.dataPunto.ciudad",
        tipo_negocio: "$pedidos.dataEstablecimiento.tipo_negocio",
        fecha_pedido: "$pedidos.createdAt",
        cant_productos: { $size: "$pedidos.productos" },
        puntos_redimidos: "$pedidos.puntos_redimidos",
        codigo_descuento: "$pedidos.codigo_descuento",
        id_pedido: "$pedidos.id_pedido",
        dataVendedores: "$dataVendedores",
        traking: "$pedidos.dataTraking",
        dataCodigos: "$pedidos.dataCodigos",
      }
    },
  )
  this.aggregate(consulta).exec(callback);
};