const mongoose = require("mongoose");
const TrabajadoresBD = require("./trabajador_model");
const moment = require("moment");
const PedidosTrakingBD = require("./pedido_tracking_model");
const PedidoBD = require("./pedido_model");
const ObjectId = require("mongoose").Types.ObjectId;
const Parametrizacion = require('./parametrizacion_model');

const Punto_entregaSchema = mongoose.Schema({
  usuario_horeca: {
    type: mongoose.Schema.Types.ObjectId,
    ref: "Usuario_horeca",
    required: true,
  },
  nombre: { type: String, required: true },
  ciudad: { type: String, required: true },
  telefono: { type: Number, required: true },
  direccion: { type: String, required: true },
  pais: { type: String, required: true, default: "" },
  departamento: { type: String, required: true, default: "" },
  coord: {
    lat: { type: String, required: false },
    lng: { type: String, required: false },
  },
  informacion_contacto: { type: Array, required: false },
  data_contacto: {
    nombreContacto: { type: String, required: false, default: "" },
    telefonoContacto: { type: String, required: false, default: ""},
    emailContacto: { type: String, required: false, default: "" },
  },
  puntos_inscripcion: { type: Number, default: 100},
  sillas: { type: Number, required: true },
  domicilios: { type: Boolean, required: true },
  numero_trabajadores: { type: Number, required: true },
  tipo_aprobador: {
    type: String,
    required: true,
    default: "No Aprobador",
    enum: ["Aprobador", "No Aprobador"],
  },
  dias_atencion: { type: Array, required: true },
  horario: { type: String, required: true },
  estado: {
    type: String,
    required: false,
    default: "Activo",
    enum: ["Activo", "Desactivado"],
  },
  formato_coordenada: {
    type: {
      type: String,
      default: 'Point'
    },
    coordinates: [Number],
    required: false
  },
  createdAt: { type: Date, required: false, default: Date.now },
});

Punto_entregaSchema.statics = {
  getAll: function (query, callback) {
    this.find(query).exec(callback);
  },
  get: function (query, callback) {
    this.findOne(query).exec(callback);
  },
  create: function (data, callback) {
    const Punto_entrega = new this(data);
    Punto_entrega.save(callback);
  },
  updateById: function (id, updateData, callback) {
    this.findOneAndUpdate(
      { _id: id },
      { $set: updateData },
      { new: true },
      callback
    );
  },
  removeAll: function (query, callback) {
    this.remove(query).exec(callback);
  },
  removeById: function (removeData, callback) {
    this.findOneAndRemove(removeData, callback);
  },
  getPuntosEntregaByHoreca: function (query, callback) {
    this.find(query).populate("usuario_horeca").exec(callback);
  },
  getHorecaByPunto: function (obj, callback) {
    this.findOne({ _id: new ObjectId(obj.punto_entrega) }).exec(callback);
  },
  getPuntosMap: function (query, callback) {
    this.aggregate([
      {
        $match: {},
      },
      {
        $lookup: {
          //from: PuntosBD.collection.name, //Nombre de la colecccion a relacionar
          from: "usuario_horecas",
          localField: "usuario_horeca", //Nombre del campo de la coleccion actual
          foreignField: "_id", //Nombre del campo de la coleccion a relacionar
          pipeline: [{ $limit: 1 }],
          as: "DataUserHoreca", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
    ]).exec(callback);
  },
  get_PuntosEstablecimientos: function (query, callback) {
    let final = moment().subtract(3, "M");
    let fecha_actual = new Date();
    let fecha_inicial = new Date(final);
    if (query === "all") {
      this.aggregate([
        {
          $match: {},
        },
        {
          $sort: { createdAt: -1 },
        },
        {
          $lookup: {
            //from: PuntosBD.collection.name, //Nombre de la colecccion a relacionar
            from: "usuario_horecas",
            localField: "usuario_horeca", //Nombre del campo de la coleccion actual
            foreignField: "_id", //Nombre del campo de la coleccion a relacionar
            pipeline: [{ $limit: 1 }],
            as: "DataUserHoreca", //Nombre del campo donde se insertara todos los documentos relacionados
          },
        },
        {
          $addFields: {
            tipoNegocioUser: "$DataUserHoreca.tipo_negocio",
          },
        },
        {
          $lookup: {
            //from: DistribuidoresBD.collection.name, //Nombre de la colecccion a relacionar
            from: "distribuidores_vinculados",
            localField: "_id", //Nombre del campo de la coleccion actual
            foreignField: "punto_entrega", //Nombre del campo de la coleccion a relacionar
            pipeline: [{ $limit: 1 }],
            as: "Distribuidores_vinculados", //Nombre del campo donde se insertara todos los documentos relacionados
          },
        },
        {
          $lookup: {
            from: TrabajadoresBD.collection.name, //Nombre de la colecccion a relacionar
            localField: "usuario_horeca", //Nombre del campo de la coleccion actual
            foreignField: "usuario_horeca", //Nombre del campo de la coleccion a relacionar
            pipeline: [
              {
                $match: {
                  tipo_trabajador: "ADMINISTRADOR APROBADOR",
                },
              },
            ],
            as: "usuariosAdministradores", //Nombre del campo donde se insertara todos los documentos relacionados
          },
        },
        {
          $lookup: {
            from: TrabajadoresBD.collection.name, //Nombre de la colecccion a relacionar
            localField: "usuario_horeca", //Nombre del campo de la coleccion actual
            foreignField: "usuario_horeca", //Nombre del campo de la coleccion a relacionar
            pipeline: [
              {
                $match: {
                  tipo_trabajador: "PLANEADOR PEDIDOS",
                },
              },
            ],
            as: "usuariosPlaneadores", //Nombre del campo donde se insertara todos los documentos relacionados
          },
        },
        {
          $lookup: {
            // from: DistribuidoresBD.collection.name, //Nombre de la colecccion a relacionar
            from: "distribuidores_vinculados",
            localField: "_id", //Nombre del campo de la coleccion actual
            foreignField: "punto_entrega", //Nombre del campo de la coleccion a relacionar
            as: "Distribuidores_total", //Nombre del campo donde se insertara todos los documentos relacionados
          },
        },
        {
          $lookup: {
            //from: PedidosBD.collection.name, //Nombre de la colecccion a relacionar
            from: "pedidos",
            localField: "usuario_horeca", //Nombre del campo de la coleccion actual
            foreignField: "usuario_horeca", //Nombre del campo de la coleccion a relacionar
            pipeline: [
              {
                $match: {
                  createdAt: {
                    $gte: fecha_inicial,
                    $lte: fecha_actual,
                  },
                },
              },
            ],
            as: "pedidosGenerales", //Nombre del campo donde se insertara todos los documentos relacionados
          },
        },
        {
          $lookup: {
            from: PedidosTrakingBD.collection.name, //Nombre de la colecccion a relacionar
            localField: "pedidosGenerales._id", //Nombre del campo de la coleccion actual
            foreignField: "pedido", //Nombre del campo de la coleccion a relacionar
            pipeline: [
              {
                $match: {
                  estado_nuevo: "Facturado",
                },
              },
            ],
            as: "TrakingpedidosGenerales", //Nombre del campo donde se insertara todos los documentos relacionados
          },
        },
        {
          $sort: { usuario_horeca: -1 },
        },
        {
          $project: {
            estado: "$estado",
            establecimiento: "$DataUserHoreca.nombre_establecimiento",
            tipo_negocio: "$DataUserHoreca.tipo_negocio",
            razon_social: "$DataUserHoreca.razon_social",
            tipo_persona: "$DataUserHoreca.tipo_usuario",
            nit: "$DataUserHoreca.nit",
            propietario: {
              $reduce: {
                input: ["$DataUserHoreca.apellidos"],
                initialValue: "$DataUserHoreca.nombres",
                in: { $concatArrays: ["$$value", "$$this"] },
              },
            },
            email: "$DataUserHoreca.correo",
            telefono: "$telefono",
            pais: "$pais",
            departamento: "$departamento",
            ciudad: "$ciudad",
            nombre_Punto: "$nombre",
            direccion: "$direccion",
            informacion_contacto: "$informacion_contacto",
            emailEncargado: "$informacion_contacto",
            telefonoEncargado: "$informacion_contacto",
            nombreAdmin: {
              $reduce: {
                input: ["$usuariosAdministradores.apellidos"],
                initialValue: "$usuariosAdministradores.nombres",
                in: { $concatArrays: ["$$value", "$$this"] },
              },
            },
            emailAdmin: "$usuariosAdministradores.correo",
            telefonoAdmin: "$usuariosAdministradores.celular",

            sillas: "$sillas",
            domi: { $cond: ["$domicilios", "Si", "No"] },
            convenio: "$Distribuidores_vinculados.convenio",
            cartera_vencida: "$Distribuidores_vinculados.pazysalvo",
            total_administradores: {
              $sum: { $size: "$usuariosAdministradores" },
            },
            total_distribuidores_vinculados: {
              $sum: { $size: "$Distribuidores_total" },
            },
            total_facturado: {
              $sum: { $size: "$TrakingpedidosGenerales" },
            },
            total_pedidos: {
              $sum: { $size: "$pedidosGenerales" },
            },
            total_planeadores: {
              $sum: { $size: "$usuariosPlaneadores" },
            },
          },
        },
      ]).exec(callback);
    } else {
      let arreglo = query.split(",");
      let busqueda = [];
      for (let s of arreglo) {
        if (s === "PANADERÍA") {
          busqueda.push({ tipoNegocioUser: "PANADERÍA / REPOSTERÍA" });
        }
        if (s === "COLEGIO") {
          busqueda.push({ tipoNegocioUser: "COLEGIO / UNIVERSIDAD / CLUB" });
        }
        if (s === "HOTEL") {
          busqueda.push({ tipoNegocioUser: "HOTEL / HOSPITAL" });
        }
        if (s === "COMEDOR") {
          busqueda.push({ tipoNegocioUser: "COMEDOR DE EMPLEADOS" });
        }
        if (s === "CENTRO") {
          busqueda.push({ tipoNegocioUser: "CENTRO DE DIVERSIÓN" });
        }
        if (s === "CAFETERIA") {
          busqueda.push({ tipoNegocioUser: "CAFETERÍA / HELADERÍA / SNACK" });
        }
        if (s === "CATERING") {
          busqueda.push({
            tipoNegocioUser: "CATERING SERVICE / SERVICIO ALIMENTACIÓN",
          });
        }
        if (s === "RESTAURANTECADENA") {
          busqueda.push({ tipoNegocioUser: "RESTAURANTE DE CADENA" });
        }
        if (s === "BADISCO") {
          busqueda.push({ tipoNegocioUser: "BAR / DISCOTECA" });
        }
        if (s == "RESTAURANTE") {
          busqueda.push({ tipoNegocioUser: "RESTAURANTE" });
        }
        if (s == "COMIDA") {
          busqueda.push({ tipoNegocioUser: "COMIDA RÁPIDA" });
        }
        if (s == "MAYORISTA") {
          busqueda.push({ tipoNegocioUser: "MAYORISTA / MINORISTA" });
        }
      }
      this.aggregate([
        {
          $match: {},
        },
        {
          $sort: { createdAt: -1 },
        },
        {
          $lookup: {
            //from: PuntosBD.collection.name, //Nombre de la colecccion a relacionar
            from: "usuario_horecas",
            localField: "usuario_horeca", //Nombre del campo de la coleccion actual
            foreignField: "_id", //Nombre del campo de la coleccion a relacionar
            pipeline: [{ $limit: 1 }],
            as: "DataUserHoreca", //Nombre del campo donde se insertara todos los documentos relacionados
          },
        },
        {
          $addFields: {
            tipoNegocioUser: "$DataUserHoreca.tipo_negocio",
          },
        },
        {
          $match: {
            $or: busqueda,
          },
        },
        {
          $lookup: {
            //from: DistribuidoresBD.collection.name, //Nombre de la colecccion a relacionar
            from: "distribuidors",
            localField: "_id", //Nombre del campo de la coleccion actual
            foreignField: "punto_entrega", //Nombre del campo de la coleccion a relacionar
            pipeline: [{ $limit: 1 }],
            as: "Distribuidores_vinculados", //Nombre del campo donde se insertara todos los documentos relacionados
          },
        },
        {
          $lookup: {
            from: TrabajadoresBD.collection.name, //Nombre de la colecccion a relacionar
            localField: "usuario_horeca", //Nombre del campo de la coleccion actual
            foreignField: "usuario_horeca", //Nombre del campo de la coleccion a relacionar
            pipeline: [
              {
                $match: {
                  tipo_trabajador: "ADMINISTRADOR APROBADOR",
                },
              },
            ],
            as: "usuariosAdministradores", //Nombre del campo donde se insertara todos los documentos relacionados
          },
        },
        {
          $lookup: {
            from: TrabajadoresBD.collection.name, //Nombre de la colecccion a relacionar
            localField: "usuario_horeca", //Nombre del campo de la coleccion actual
            foreignField: "usuario_horeca", //Nombre del campo de la coleccion a relacionar
            pipeline: [
              {
                $match: {
                  tipo_trabajador: "PLANEADOR PEDIDOS",
                },
              },
            ],
            as: "usuariosPlaneadores", //Nombre del campo donde se insertara todos los documentos relacionados
          },
        },
        {
          $lookup: {
            // from: DistribuidoresBD.collection.name, //Nombre de la colecccion a relacionar
            from: "distribuidores_vinculados",
            localField: "_id", //Nombre del campo de la coleccion actual
            foreignField: "punto_entrega", //Nombre del campo de la coleccion a relacionar
            as: "Distribuidores_total", //Nombre del campo donde se insertara todos los documentos relacionados
          },
        },
        {
          $lookup: {
            //from: PedidosBD.collection.name, //Nombre de la colecccion a relacionar
            from: "pedidos",
            localField: "usuario_horeca", //Nombre del campo de la coleccion actual
            foreignField: "usuario_horeca", //Nombre del campo de la coleccion a relacionar
            pipeline: [
              {
                $match: {
                  createdAt: {
                    $gte: fecha_inicial,
                    $lte: fecha_actual,
                  },
                },
              },
            ],
            as: "pedidosGenerales", //Nombre del campo donde se insertara todos los documentos relacionados
          },
        },
        {
          $lookup: {
            from: PedidosTrakingBD.collection.name, //Nombre de la colecccion a relacionar
            localField: "pedidosGenerales._id", //Nombre del campo de la coleccion actual
            foreignField: "pedido", //Nombre del campo de la coleccion a relacionar
            pipeline: [
              {
                $match: {
                  estado_nuevo: "Facturado",
                },
              },
            ],
            as: "TrakingpedidosGenerales", //Nombre del campo donde se insertara todos los documentos relacionados
          },
        },
        {
          $sort: { usuario_horeca: -1 },
        },
        {
          $project: {
            estado: "$estado",
            establecimiento: "$DataUserHoreca.nombre_establecimiento",
            tipo_negocio: "$DataUserHoreca.tipo_negocio",
            razon_social: "$DataUserHoreca.razon_social",
            tipo_persona: "$DataUserHoreca.tipo_usuario",
            nit: "$DataUserHoreca.nit",
            propietario: {
              $reduce: {
                input: ["$DataUserHoreca.apellidos"],
                initialValue: "$DataUserHoreca.nombres",
                in: { $concatArrays: ["$$value", "$$this"] },
              },
            },
            email: "$DataUserHoreca.correo",
            telefono: "$telefono",
            pais: "$pais",
            departamento: "$departamento",
            ciudad: "$ciudad",
            nombre_Punto: "$nombre",
            direccion: "$direccion",
            nombreAdmin: {
              $reduce: {
                input: ["$usuariosAdministradores.apellidos"],
                initialValue: "$usuariosAdministradores.nombres",
                in: { $concatArrays: ["$$value", "$$this"] },
              },
            },
            emailAdmin: "$usuariosAdministradores.correo",
            telefonoAdmin: "$usuariosAdministradores.celular",

            sillas: "$sillas",
            domi: { $cond: ["$domicilios", "Si", "No"] },
            convenio: "$Distribuidores_vinculados.convenio",
            cartera_vencida: "$Distribuidores_vinculados.pazysalvo",
            total_administradores: {
              $sum: { $size: "$usuariosAdministradores" },
            },
            total_distribuidores_vinculados: {
              $sum: { $size: "$Distribuidores_total" },
            },
            total_facturado: {
              $sum: { $size: "$TrakingpedidosGenerales" },
            },
            total_pedidos: {
              $sum: { $size: "$pedidosGenerales" },
            },
            total_planeadores: {
              $sum: { $size: "$usuariosPlaneadores" },
            },
          },
        },
      ]).exec(callback);
    }
  },
  getAllPuntosHorecaDistribuidor: function (query, callback) {
    this.aggregate([
      {
        $match: {
          usuario_horeca: new ObjectId(query.horeca),
        },
      },
    ]).exec(callback);
  },
  getPuntosMiniatura: function (query, callback) {
    this.aggregate([
      {
        $match: {
          usuario_horeca: new ObjectId(query._id),
        },
      },
      {
        $lookup: {
          from: "pedidos",
          localField: "_id", //Nombre del campo de la coleccion actual
          foreignField: "punto_entrega", //Nombre del campo de la coleccion a relacionar
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
          ],
          as: "data_pedidos", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
      {
        $project: {
          _id: "$_id",
          nombre: "$nombre",
          estado: "$estado",
          direccion: "$direccion",
          ciudad: "$ciudad",
          horario: "$horario",
          dias_atencion: "$dias_atencion",
          pedidos: { $size: "$data_pedidos" },
        },
      },
      {
        $facet: {
          activos: [
            {
              $match: { estado: "Activo" },
            },
          ],
          desactivados: [
            {
              $match: { estado: "Desactivado" },
            },
          ],
        },
      },
    ]).exec(callback);
  },
  getEstablecimientos_por_sillas: function (query, callback) {
    this.aggregate([
      {
        $facet: {
          sillas0: [
            {
              $match: {
                sillas: 0
              }
            }
          ],
          sillas0_10: [
            {
              $match: {
                sillas: {
                  $gte: 1,
                  $lte: 10,
                },
              },
            },
          ],
          sillas11_40: [
            {
              $match: {
                sillas: {
                  $gte: 11,
                  $lte: 40,
                },
              },
            },
          ],
          sillas41_80: [
            {
              $match: {
                sillas: {
                  $gte: 41,
                  $lte: 80,
                },
              },
            },
          ],
          sillas81_150: [
            {
              $match: {
                sillas: {
                  $gte: 81,
                  $lte: 151,
                },
              },
            },
          ],
          sillas151_300: [
            {
              $match: {
                sillas: {
                  $gte: 151,
                  $lte: 300,
                },
              },
            },
          ],
          sillas301_500: [
            {
              $match: {
                sillas: {
                  $gte: 301,
                  $lte: 500,
                },
              },
            },
          ],
          sillas501: [
            {
              $match: {
                sillas: {
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
              label: "0",
              count: { $size: "$sillas0" }
            },
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
  getEstablecimientos_por_ciudad: function (query, callback) {
    this.aggregate([
      {
        $group: {
          _id: "$ciudad",
          cantidad: { $sum: 1 },
        },
      },
    ]).exec(callback);
  },
  getEstablecimientos_por_tipo: function (query, callback) {
    this.aggregate([
      {
        $lookup: {
          //from: PuntosBD.collection.name, //Nombre de la colecccion a relacionar
          from: "usuario_horecas",
          localField: "usuario_horeca", //Nombre del campo de la coleccion actual
          foreignField: "_id", //Nombre del campo de la coleccion a relacionar
          pipeline: [{ $limit: 1 }],
          as: "DataUserHoreca", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
      {
        $group: {
          _id: "$DataUserHoreca.tipo_negocio",
          cantidad: { $sum: 1 },
        },
      },
    ]).exec(callback);
  },
  getEstablecimientos_por_mes: function (query, callback) {
    this.find({
      createdAt: {
        $gte: new Date(query.inicio),
        $lte: new Date(query.fin),
      },
    }).exec(callback);
  },
  getEstablecimientos_por_mes_acumulados: async function (fecha_anterior, callback) {
    const puntos_previos = await this.countDocuments({ createdAt: { $lt: new Date(fecha_anterior) } })
    this.aggregate([
      {
        $addFields: {
          fecha_punto: {
            $dateToString: {
              format: "%Y-%m",
              date: "$createdAt",
            },
          },
        },
      },
      {
        $match: {
          createdAt: { $gte: new Date(fecha_anterior) }
        }
      },
      {
        $group: {
          _id: "$fecha_punto",
          total: { $sum: 1 },
        },
      },
      {
        $setWindowFields: {
          "sortBy": {
              "_id": 1
          },
          "output": {
              "cumulative": {
                  "$sum": "$total",
                  "window": {
                      "documents": [
                          "unbounded",
                          "current"
                      ]
                  }
              }
          }
        }
      },
      {
        $project: {
          mes: "$_id",
          cantidad: { $add: [puntos_previos, "$cumulative"] }
        }
      },
      {
        $sort: { _id: 1 }
      }
    ]).exec(callback)
  },
  getEstablecimientos_con_domicilio: function (query, callback) {
    this.aggregate([
      {
        $match: {},
      },
      {
        $group: {
          _id: "$domicilios",
          cantidad: { $sum: 1 },
        },
      },
    ]).exec(callback);
  },
  getPuntosAdicionalesByUsuarioHoreca: function (req, callback) {
    const query = {
      usuario_horeca: new ObjectId(req.trim()),
      estado: "Activo",
    };
    this.aggregate([
      {
        $match: query,
      },
      {
        $addFields: {
          nombre: "$nombre",
          sillas: "$sillas",
          direccion: "$direccion",
          ciudad: "$ciudad",
          horario: "$horario",
        },
      },
      {
        $project: {
          _id: 1,
          nombre: 1,
          sillas: 1,
          direccion: 1,
          ciudad: 1,
          horario: 1,
        },
      },
    ]).exec(callback);
  },
  /** Retorna la cantidad de trabajadores pendientes de un horeca */
  getVinculacionesPendientes: function (req, callback) {
    this.aggregate([
      {
        $match: {
          usuario_horeca: new ObjectId(req.idHoreca),
          estado: "Activo",
        },
      },
      {
        $project: {
          punto_id: "$_id",
          usuario_horeca: "$usuario_horeca",
        },
      },
      {
        $lookup: {
          from: "distribuidores_vinculados", //Nombre de la colecccion a relacionar
          localField: "punto_id", //Nombre del campo de la coleccion actual
          foreignField: "punto_entrega", //Nombre del campo de la coleccion a relacionar
          pipeline: [
            {
              $match: {
                estado: "Pendiente",
              },
            },
          ],
          as: "data_vinculacion", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
      {
        $project: {
          usuario_horeca: "$usuario_horeca",
          vinculacion: { $arrayElemAt: ["$data_vinculacion", 0] },
        },
      },
      {
        $group: {
          _id: "$vinculacion.distribuidor",
          horeca: {
            $first: "$usuario_horeca",
          },
          total: { $sum: 1 },
        },
      },
      {
        $group: {
          _id: "$horeca",
          total: { $sum: 1 },
        },
      },
    ]).exec(callback);
  },
  /** Tabla Puntos ganados por producto-establecimiento */
  getPuntosGanadosProductoEstablecimiento: async function (req, callback) {
    const parametrizacionData = await Parametrizacion.findOne();
    const valor_punto_feat = parametrizacionData.valor_1puntoft;
    this.aggregate([
      {
        $match: {
          _id: new ObjectId(req.usuario_horeca),
        },
      },
      {
        $lookup: {
          from: "puntos_ganados_establecimientos", //Nombre de la colecccion a relacionar
          localField: "_id", //Nombre del campo de la coleccion actual
          foreignField: "punto_entrega", //Nombre del campo de la coleccion a relacionar
          as: "data_puntos_feat", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
      {
        $unwind: "$data_puntos_feat",
      },
      {
        $replaceRoot: {
          newRoot: "$data_puntos_feat",
        },
      },
     
      {
        $lookup: {
          from: "reportepedidos", //Nombre de la colecccion a relacionar
          localField: "pedido", //Nombre del campo de la coleccion actual
          foreignField: "idPedido", //Nombre del campo de la coleccion a relacionar
          pipeline: [
            {
              $lookup: {
                from: "pedidos", //Nombre de la colecccion a relacionar
                localField: "idPedido", //Nombre del campo de la coleccion actual
                foreignField: "_id", //Nombre del campo de la coleccion a relacionar
                pipeline: [
                  {
                    $project: {
                      fecha_pedido: "$fecha",
                      id_pedido: "$id_pedido",
                      pedido_createdAt: "$createdAt",
                      subtotal: "$subtotal_pedido",
                      estado_pedido: "$estado"
                    },
                  },
                ],
                as: "data_pedido", //Nombre del campo donde se insertara todos los documentos relacionados
              },
            },
            {
              $addFields: {
                pedido_createdAt: {
                  $arrayElemAt: ["$data_pedido.pedido_createdAt", 0],
                },
                fecha_cierre_puntosft: "$detalleProducto.fecha_cierre_puntosft",
                fecha_apertura_puntosft:
                  "$detalleProducto.fecha_apertura_puntosft",
              },
            },
            {
              $lookup: {
                from: "distribuidors", //Nombre de la colecccion a relacionar
                localField: "idDistribuidor", //Nombre del campo de la coleccion actual
                foreignField: "_id", //Nombre del campo de la coleccion a relacionar
                pipeline: [
                  {
                    $project: {
                      distribuidor_nombre: "$nombre",
                      distribuidor_nit: "$nit_cc"
                    },
                  },
                ],
                as: "data_distribuidor", //Nombre del campo donde se insertara todos los documentos relacionados
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
                      punto_nombre: "$nombre",
                      punto_horeca: "$usuario_horeca",
                    },
                  },
                ],
                as: "data_punto", //Nombre del campo donde se insertara todos los documentos relacionados
              },
            },
            {
              $project: {
                idPunto: "$idPunto",
                idPedido: "$idPedido",
                puntosGanados: "$puntosGanados",
                puntos_ganados: "$puntos_ganados",
                producto_id: "$productoId",
                pedido_createdAt: "$pedido_createdAt",
                fecha_cierre_puntosft: "$fecha_cierre_puntosft",
                fecha_apertura_puntosft: "$fecha_apertura_puntosft",
                producto_nombre: "$nombreProducto",
                producto_unidades_compradas: "$unidadesCompradas",
                descuento: "$descuento",
                producto_precios: {
                  $arrayElemAt: ["$detalleProducto.precios", 0],
                },
                pedido_ID: {
                  $arrayElemAt: ["$data_pedido.id_pedido", 0],
                },
                pedido_fecha: {
                  $arrayElemAt: ["$data_pedido.fecha_pedido", 0],
                },
                distribuidor_nombre: {
                  $arrayElemAt: ["$data_distribuidor.distribuidor_nombre", 0],
                },
                punto_nombre: {
                  $arrayElemAt: ["$data_punto.punto_nombre", 0],
                },
                distribuidor_nit: {
                  $arrayElemAt: ["$data_distribuidor.distribuidor_nit", 0],
                },
                subtotal_pedido: {
                  $arrayElemAt: ["$data_pedido.subtotal", 0],
                },
                estado_pedido: {
                  $arrayElemAt: ["$data_pedido.estado_pedido", 0],
                },
              },
            },
            {
              $addFields: {
                total_puntos: {
                  $multiply: [
                    "$producto_unidades_compradas",
                    "$producto_precios.puntos_ft_unidad",
                  ],
                },
                fecha_cierre_puntosft: '',
                fecha_apertura_puntosft: '',
                /*fecha_cierre_puntosft: {
                  $dateFromString: {
                    dateString: "$fecha_cierre_puntosft",
                  },
                },
                fecha_apertura_puntosft: {
                  $dateFromString: {
                    dateString: "$fecha_apertura_puntosft",
                  },
                },*/
                valor_punto_feat: valor_punto_feat
              },
            },
            
          ],
          as: "data_tabla", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
      {
        $unwind: "$data_tabla",
      },
      {
        $replaceRoot: {
          newRoot: "$data_tabla",
        },
      },
      {
        $group: {
          _id: '$pedido_ID',
          punto: { $first: '$punto_nombre' },
          distribuidor: { $first: '$distribuidor_nombre' },
          nit: { $first: '$distribuidor_nit' },
          fecha: { $first: '$pedido_fecha' },
          valor_pedido: { $first: '$subtotal_pedido' },
          estado: { $first: '$estado_pedido' },
          descuento: { $first: '$descuento' },
          acumulados: { $first: '$puntosGanados' },
          valor_punto_feat: { $first: '$valor_punto_feat' }
        }
      },
      {
        $sort: { fecha: -1 },
      },
    ]).exec(callback);
  },
  /** Grafica barra puntos ganados horeca por mes */
  getPuntosGanadosPorMes: function (query, callback) {
    this.aggregate([
      {
        $match: {
          _id: new ObjectId(query.usuario_horeca),
        },
      },
      /*{
        $project: {
          punto_id: "$_id",
        },
      },*/
      {
        $lookup: {
          from: "puntos_ganados_establecimientos", //Nombre de la colecccion a relacionar
          localField: "_id", //Nombre del campo de la coleccion actual
          foreignField: "punto_entrega", //Nombre del campo de la coleccion a relacionar
          as: "data_puntos_feat", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
      {
        $unwind: "$data_puntos_feat",
      },
      {
        $replaceRoot: {
          newRoot: "$data_puntos_feat",
        },
      },
      {
        $match: {
          movimiento: { $in: ["Congelados", "CodigoGenerado"] },
          fecha: {
            $gte: new Date(query.inicio),
            $lte: new Date(query.fin),
          },
        },
      },
      {
        $addFields: {
          fechaGroup: {
            $dateToString: {
              format: "%Y-%m",
              date: "$fecha",
            },
          },
        },
      },
      {
        $group: {
          _id: "$fechaGroup",
          total: { $sum: "$puntos_ganados" },
        },
      },
      {
        $sort: { _id: 1 },
      },
    ]).exec(callback);
  },
  /** Grafica barra puntos ganados por distribuidor */
  getPuntosGanadosPorDistribuidor: function (query, callback) {
    this.aggregate([
      {
        $match: {
          _id: new ObjectId(query.usuario_horeca),
        },
      },
      /*{
        $project: {
          punto_id: "$_id",
        },
      },*/
      {
        $lookup: {
          from: "puntos_ganados_establecimientos", //Nombre de la colecccion a relacionar
          localField: "_id", //Nombre del campo de la coleccion actual
          foreignField: "punto_entrega", //Nombre del campo de la coleccion a relacionar
          as: "data_puntos_feat", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
      {
        $unwind: "$data_puntos_feat",
      },
      {
        $replaceRoot: {
          newRoot: "$data_puntos_feat",
        },
      },
      {
        $match: {
          movimiento: { $in: ["Congelados", "CodigoGenerado"] },
          fecha: {
            $gte: new Date(query.inicio),
            $lte: new Date(query.fin),
          },
        },
      },
      {
        $lookup: {
          from: "pedidos", //Nombre de la colecccion a relacionar
          localField: "pedido", //Nombre del campo de la coleccion actual
          foreignField: "_id", //Nombre del campo de la coleccion a relacionar
          pipeline: [
            {
              $project: {
                distribuidor: "$distribuidor",
              },
            },
          ],
          as: "data_pedido", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
      {
        $lookup: {
          from: "distribuidors", //Nombre de la colecccion a relacionar
          localField: "data_pedido.distribuidor", //Nombre del campo de la coleccion actual
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
          _id: {
            $arrayElemAt: ["$data_distribuidor.distribuidor_nombre", 0],
          },
          total: { $sum: "$puntos_ganados" },
        },
      },
      {
        $sort: { total: -1 },
      },
    ]).exec(callback);
  },
  /** Grafica barra puntos ganados por organizacion */
  getPuntosGanadosPorOrganizacion: function (query, callback) {
    this.aggregate([
      {
        $match: {
          _id: new ObjectId(query.usuario_horeca),
        },
      },
      /*
      {
        $project: {
          punto_id: "$_id",
        },
      },
      */
      {
        $lookup: {
          from: "puntos_ganados_establecimientos", //Nombre de la colecccion a relacionar
          localField: "_id", //Nombre del campo de la coleccion actual
          foreignField: "punto_entrega", //Nombre del campo de la coleccion a relacionar
          as: "data_puntos_feat", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
      {
        $unwind: "$data_puntos_feat",
      },
      {
        $replaceRoot: {
          newRoot: "$data_puntos_feat",
        },
      },
      {
        $match: {
          movimiento: { $in: ["Congelados", "CodigoGenerado"] },
          fecha: {
            $gte: new Date(query.inicio),
            $lte: new Date(query.fin),
          },
        },
      },
      {
        $lookup: {
          from: "reportepedidos", //Nombre de la colecccion a relacionar
          localField: "pedido", //Nombre del campo de la coleccion actual
          foreignField: "idPedido", //Nombre del campo de la coleccion a relacionar
          as: "data_pedido", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
      {
        $unwind: "$data_pedido",
      },
      {
        $replaceRoot: {
          newRoot: "$data_pedido",
        },
      },
      {
        $project: {
          producto_precios: {
            $arrayElemAt: ["$detalleProducto.precios", 0],
          },
          producto_unidades_compradas: "$unidadesCompradas",
          organizacion_id: "$idOrganizacion",
        },
      },
      {
        $addFields: {
          total_puntos: {
            $multiply: [
              "$producto_unidades_compradas",
              "$producto_precios.puntos_ft_unidad",
            ],
          },
        },
      },
      {
        $group: {
          _id: "$organizacion_id",
          total: { $sum: "$total_puntos" },
        },
      },
      {
        $lookup: {
          from: "organizacions", //Nombre de la colecccion a relacionar
          localField: "_id", //Nombre del campo de la coleccion actual
          foreignField: "_id", //Nombre del campo de la coleccion a relacionar
          pipeline: [
            {
              $project: {
                organizacion_nombre: "$nombre",
              },
            },
          ],
          as: "data_organizacion", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
      {
        $project: {
          _id: {
            $arrayElemAt: ["$data_organizacion.organizacion_nombre", 0],
          },
          total: "$total",
        },
      },
      {
        $sort: { total: -1 },
      },
    ]).exec(callback);
  },
  /** Tabla Puntos REDIMIDOS por producto-establecimiento */
  getPuntosRedimidosProductoEstablecimiento: function (req, callback) {
    this.aggregate([
      {
        $match: {
          _id: new ObjectId(req.usuario_horeca),
        },
      },
      /*{
        $project: {
          punto_id: "$_id",
        },
      },*/
      {
        $lookup: {
          from: "codigos_descuento_generados", //Nombre de la colecccion a relacionar
          localField: "_id", //Nombre del campo de la coleccion actual
          foreignField: "punto_entrega", //Nombre del campo de la coleccion a relacionar
          as: "data_puntos_feat", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
      {
        $unwind: "$data_puntos_feat",
      },
      {
        $replaceRoot: {
          newRoot: "$data_puntos_feat",
        },
      },
      {
        $match: {
          estado: "Redimido" 
        },
      },
      {
        $lookup: {
          from: "reportepedidos", //Nombre de la colecccion a relacionar
          localField: "idPedido", //Nombre del campo de la coleccion actual
          foreignField: "idPedido", //Nombre del campo de la coleccion a relacionar
           pipeline: [
            /*{
              $lookup: {
                from: "pedidos", //Nombre de la colecccion a relacionar
                localField: "idPedido", //Nombre del campo de la coleccion actual
                foreignField: "_id", //Nombre del campo de la coleccion a relacionar
                pipeline: [
                  {
                    $project: {
                      fecha_pedido: "$fecha",
                      id_pedido: "$id_pedido",
                      codigo_descuento: "$codigo_descuento",
                      pedido_createdAt: "$createdAt",
                      puntosGanados: "$puntosGanados",
                     
                    },
                  },
                ],
                as: "data_pedido", //Nombre del campo donde se insertara todos los documentos relacionados
              },
            },*/
          ],
          as: "data_tabla", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
      {
        $addFields: {
          pedido_createdAt: {
            $arrayElemAt: ["$data_tabla.createdAt", 0],
          }
        },
      },
      {
        $lookup: {
          from: "distribuidors", //Nombre de la colecccion a relacionar
          localField: "data_tabla.idDistribuidor", //Nombre del campo de la coleccion actual
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
        $lookup: {
          from: "punto_entregas",
          localField: "data_tabla.idPunto", //Nombre del campo de la coleccion actual
          foreignField: "_id", //Nombre del campo de la coleccion a relacionar
          pipeline: [
            {
              $project: {
                punto_nombre: "$nombre",
                punto_horeca: "$usuario_horeca",
              },
            },
          ],
          as: "data_punto", //Nombre del campo donde se insertara todos los documentos relacionados
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
                id_pedido: "$id_pedido",
              },
            },
          ],
          as: "data_pedido", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
      {
        $project: {
          distribuidor:'$data_distribuidor.distribuidor_nombre',
          totalPedido:'$data_tabla.totalCompra',
          descuentoTotal:'$data_tabla.descuento',
          subtotalCompra:'$data_tabla.subtotalCompra',
          id_pedido:'$data_pedido.id_pedido',
          codigo_usado:'$codigo_creado',
          valor_moneda:'$valor_moneda',
          punto_ft_codigo:'$valor_paquete',
          fecha_pedido:'$fecha',
        },
      },
      {
        $sort: { fecha_pedido: -1 }
      },
    ]).exec(callback);
  },
  /** Grafica barra puntos REDIMIDOS horeca por mes */
  getPuntosRedimidosPorMes: function (query, callback) {
    this.aggregate([
      {
        $match: {
          _id: new ObjectId(query.usuario_horeca),
        },
      },
      /*{
        $project: {
          punto_id: "$_id",
        },
      },*/
      {
        $lookup: {
          from: "puntos_ganados_establecimientos", //Nombre de la colecccion a relacionar
          localField: "_id", //Nombre del campo de la coleccion actual
          foreignField: "punto_entrega", //Nombre del campo de la coleccion a relacionar
          as: "data_puntos_feat", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
      {
        $unwind: "$data_puntos_feat",
      },
      {
        $replaceRoot: {
          newRoot: "$data_puntos_feat",
        },
      },
      {
        $match: {
          movimiento: "CodigoGenerado",
          fecha: {
            $gte: new Date(query.inicio),
            $lte: new Date(query.fin),
          },
        },
      },
      {
        $addFields: {
          fechaGroup: {
            $dateToString: {
              format: "%Y-%m",
              date: "$fecha",
            },
          },
        },
      },
      {
        $group: {
          _id: "$fechaGroup",
          total: { $sum: "$puntos_ganados" },
        },
      },
      {
        $sort: { _id: 1 },
      },
    ]).exec(callback);
  },
  /** Grafica barra puntos REDIMIDOS por distribuidor */
  getPuntosRedimidosPorDistribuidor: function (query, callback) {
    this.aggregate([
      {
        $match: {
          _id: new ObjectId(query.usuario_horeca),
        },
      },
      /*{
        $project: {
          punto_id: "$_id",
        },
      },*/
      {
        $lookup: {
          from: "puntos_ganados_establecimientos", //Nombre de la colecccion a relacionar
          localField: "_id", //Nombre del campo de la coleccion actual
          foreignField: "punto_entrega", //Nombre del campo de la coleccion a relacionar
          as: "data_puntos_feat", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
      {
        $unwind: "$data_puntos_feat",
      },
      {
        $replaceRoot: {
          newRoot: "$data_puntos_feat",
        },
      },
      {
        $match: {
          movimiento: "CodigoGenerado",
          fecha: {
            $gte: new Date(query.inicio),
            $lte: new Date(query.fin),
          },
        },
      },
      {
        $lookup: {
          from: "pedidos", //Nombre de la colecccion a relacionar
          localField: "pedido", //Nombre del campo de la coleccion actual
          foreignField: "_id", //Nombre del campo de la coleccion a relacionar
          pipeline: [
            {
              $project: {
                distribuidor: "$distribuidor",
              },
            },
          ],
          as: "data_pedido", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
      {
        $lookup: {
          from: "distribuidors", //Nombre de la colecccion a relacionar
          localField: "data_pedido.distribuidor", //Nombre del campo de la coleccion actual
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
          _id: {
            $arrayElemAt: ["$data_distribuidor.distribuidor_nombre", 0],
          },
          total: { $sum: "$puntos_ganados" },
        },
      },
      {
        $sort: { total: -1 },
      },
    ]).exec(callback);
  },
  /** Grafica barra puntos REDIMIDOS por organizacion */
  getPuntosRedimidosPorOrganizacion: function (query, callback) {
    this.aggregate([
      {
        $match: {
          _id: new ObjectId(query.usuario_horeca),
        },
      },
      /*{
        $project: {
          punto_id: "$_id",
        },
      },*/
      {
        $lookup: {
          from: "puntos_ganados_establecimientos", //Nombre de la colecccion a relacionar
          localField: "_id", //Nombre del campo de la coleccion actual
          foreignField: "punto_entrega", //Nombre del campo de la coleccion a relacionar
          as: "data_puntos_feat", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
      {
        $unwind: "$data_puntos_feat",
      },
      {
        $replaceRoot: {
          newRoot: "$data_puntos_feat",
        },
      },
      {
        $match: {
          movimiento: "CodigoGenerado",
          fecha: {
            $gte: new Date(query.inicio),
            $lte: new Date(query.fin),
          },
        },
      },
      {
        $lookup: {
          from: "reportepedidos", //Nombre de la colecccion a relacionar
          localField: "pedido", //Nombre del campo de la coleccion actual
          foreignField: "idPedido", //Nombre del campo de la coleccion a relacionar
          as: "data_pedido", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
      {
        $unwind: "$data_pedido",
      },
      {
        $replaceRoot: {
          newRoot: "$data_pedido",
        },
      },
      {
        $project: {
          producto_precios: {
            $arrayElemAt: ["$detalleProducto.precios", 0],
          },
          producto_unidades_compradas: "$unidadesCompradas",
          organizacion_id: "$idOrganizacion",
        },
      },
      {
        $addFields: {
          total_puntos: {
            $multiply: [
              "$producto_unidades_compradas",
              "$producto_precios.puntos_ft_unidad",
            ],
          },
        },
      },
      {
        $group: {
          _id: "$organizacion_id",
          total: { $sum: "$total_puntos" },
        },
      },
      {
        $lookup: {
          from: "organizacions", //Nombre de la colecccion a relacionar
          localField: "_id", //Nombre del campo de la coleccion actual
          foreignField: "_id", //Nombre del campo de la coleccion a relacionar
          pipeline: [
            {
              $project: {
                organizacion_nombre: "$nombre",
              },
            },
          ],
          as: "data_organizacion", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
      {
        $project: {
          _id: {
            $arrayElemAt: ["$data_organizacion.organizacion_nombre", 0],
          },
          total: "$total",
        },
      },
      {
        $sort: { total: -1 },
      },
    ]).exec(callback);
  },
  /********************************* Graficas Horeca ********************************/
  /** Tabla vinculacion punto de entrega-distribuidor */
  getInformeHorecaTablaPuntosDistribuidor2: function (query, callback) {
    this.aggregate([
      /** REcupera los puntos asociados al horeca */
      {
        $match: {
          usuario_horeca: new ObjectId(query.idHoreca),
        },
      },
      /** Reduce el peso del objeto solo dejan do el ID y el nombre */
      {
        $project: {
          punto_id: "$_id",
          punto_nombre: "$nombre",
        },
      },
      /** Busca todos los distribuidores asociados al punto */
      {
        $lookup: {
          from: "distribuidores_vinculados", //Nombre de la colecccion a relacionar
          localField: "punto_id", //Nombre del campo de la coleccion actual
          foreignField: "punto_entrega", //Nombre del campo de la coleccion a relacionar
          pipeline: [
            /** Reduce la data de vinculacion entre punto y distribuidor */
            {
              $project: {
                punto_id: "$punto_entrega",
                punto_id2: "$punto_entrega",
                distribuidor_id: "$distribuidor",
                estado: "$estado",
                convenio: "$convenio",
                cartera: "$cartera",
                pazysalvo: "$pazysalvo",
                vendedor: "$vendedor",
              },
            },
            /** Equipo comercial asignado */
            {
              $lookup: {
                from: "trabajadors",
                localField: "vendedor", //Nombre del campo de la coleccion actual
                foreignField: "_id", //Nombre del campo de la coleccion a relacionar
                pipeline: [
                  {
                    $project: {
                      vendedor_nombre: {
                        $concat: ["$nombres", " ", "$apellidos"],
                      },
                    },
                  },
                ],
                as: "data_vendedores", //Nombre del campo donde se insertara todos los documentos relacionados
              },
            },
            /** Arma la Info general de cada distribuidor */
            {
              $lookup: {
                from: "distribuidors",
                localField: "distribuidor_id", //Nombre del campo de la coleccion actual
                foreignField: "_id", //Nombre del campo de la coleccion a relacionar
                pipeline: [
                  {
                    $project: {
                      distribuidor_id: "$_id",
                      distribuidor_nombre: "$nombre",
                      distribuidor_pais: "$pais",
                      distribuidor_departamento: "$departamento",
                      distribuidor_ciudad: "$ciudad",
                      distribuidor_nit_cc: "$nit_cc",
                      distribuidor_tiempo_entrega: "$tiempo_entrega",
                      distribuidor_tipo: "$tipo",
                      distribuidor_valor_minimo_pedido: "$valor_minimo_pedido",
                      distribuidor_razon_social: "$razon_social",
                      distribuidor_tipo_persona: "$tipo_persona",
                    },
                  },
                ],
                as: "data_distribuidor", //Nombre del campo donde se insertara todos los documentos relacionados
              },
            },
            /** Busca el totl  pedidos que se hayan entregado entre dist y punto  */
            {
              $lookup: {
                from: "reportepedidos",
                let: {
                  first: "$punto_id",
                  second: "$distribuidor_id",
                },
                pipeline: [
                  /** Retorna solo las compras entre punto y distribuidor */
                  {
                    $match: {
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
                  /** Agrupa los objetos por pedidos ((evita que se cuente el mismo pedido varias veces)) */
                  {
                    $group: {
                      _id: "$idPedido",
                    },
                  },
                  /** Buscamos el estado actual del pedido */
                  {
                    $lookup: {
                      from: "pedidos",
                      localField: "_id", //Nombre del campo de la coleccion actual
                      foreignField: "_id", //Nombre del campo de la coleccion a relacionar
                     
                      as: "data_pedido", //Nombre del campo donde se insertara todos los documentos relacionados
                    },
                  },
                  /** Filtra y solo deja de entregado hacia adelante */
                
                  /** Crea un estado falso para agruparlos por este campo y contar la cantidad de pedidos */
                  {
                    $project: {
                      pedido_calificacion: {
                        $arrayElemAt: ["$data_pedido.calificacion", 0],
                      },
                      pedido_estado: {
                        $arrayElemAt: ["$data_pedido.estado", 0],
                      },
                      estado_vacio: "vacio",
                    },
                  },
                  {
                    $group: {
                      _id: "$estado_vacio",
                      pedidos_total: { $sum: 1 },
                      /** Agrega todos los pedidos entre el punto y el distribuidor para buscar las cailicaciones */
                      pedido_calificacion: { $push: "$$ROOT" },
                    },
                  },
                ],
                as: "data_compras",
              },
            },
          ],
          as: "data_tabla", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
      {
        $project: {
          punto_nombre: "$punto_nombre",
          vinculacion_estado: {
            $arrayElemAt: ["$data_tabla.estado", 0],
          },
          vinculacion_convenio: {
            $arrayElemAt: ["$data_tabla.convenio", 0],
          },
          vinculacion_cartera: {
            $arrayElemAt: ["$data_tabla.cartera", 0],
          },
          vinculacion_pazysalvo: {
            $arrayElemAt: ["$data_tabla.pazysalvo", 0],
          },
          data_tabla: {
            $arrayElemAt: ["$data_tabla", 0],
          },
        },
      },
      {
        $project: {
          punto_nombre: "$punto_nombre",
          vinculacion_estado: "$vinculacion_estado",
          vinculacion_convenio: "$vinculacion_convenio",
          vinculacion_cartera: "$vinculacion_cartera",
          vinculacion_pazysalvo: "$vinculacion_pazysalvo",
          pedidos_entregados_total: {
            $arrayElemAt: ["$data_tabla.data_compras.pedidos_total", 0],
          },
          pedido_todos_calificados: {
            $arrayElemAt: ["$data_tabla.data_compras", 0],
          },
          vendedor_nombre: "$data_tabla.data_vendedores.vendedor_nombre",
          distribuidor_id: {
            $arrayElemAt: ["$data_tabla.data_distribuidor.distribuidor_id", 0],
          },
          distribuidor_nombre: {
            $arrayElemAt: [
              "$data_tabla.data_distribuidor.distribuidor_nombre",
              0,
            ],
          },
          distribuidor_departamento: {
            $arrayElemAt: [
              "$data_tabla.data_distribuidor.distribuidor_departamento",
              0,
            ],
          },
          distribuidor_ciudad: {
            $arrayElemAt: [
              "$data_tabla.data_distribuidor.distribuidor_ciudad",
              0,
            ],
          },
          distribuidor_nit_cc: {
            $arrayElemAt: [
              "$data_tabla.data_distribuidor.distribuidor_nit_cc",
              0,
            ],
          },
          distribuidor_tiempo_entrega: {
            $arrayElemAt: [
              "$data_tabla.data_distribuidor.distribuidor_tiempo_entrega",
              0,
            ],
          },
          distribuidor_tipo: {
            $arrayElemAt: [
              "$data_tabla.data_distribuidor.distribuidor_tipo",
              0,
            ],
          },
          distribuidor_valor_minimo_pedido: {
            $arrayElemAt: [
              "$data_tabla.data_distribuidor.distribuidor_valor_minimo_pedido",
              0,
            ],
          },
          distribuidor_razon_social: {
            $arrayElemAt: [
              "$data_tabla.data_distribuidor.distribuidor_razon_social",
              0,
            ],
          },
          distribuidor_tipo_persona: {
            $arrayElemAt: [
              "$data_tabla.data_distribuidor.distribuidor_tipo_persona",
              0,
            ],
          },
        },
      },
      /** Se agrega este campo para un sort no-case-sensitive */
      {
        $addFields: {
          label_to_sort: {
            $toLower: "$distribuidor_nombre",
          },
          label_to_sort_2: {
            $toLower: "$punto_nombre",
          },
        },
      },
      {
        $sort: { label_to_sort: 1, label_to_sort_2: 1 },
      },
    ]).exec(callback);
  },
  getInformeHorecaTablaPuntosDistribuidor: function (query, callback) {
    this.aggregate([
      /** REcupera los puntos asociados al horeca */
      {
        $match: {
          usuario_horeca: new ObjectId(query.idHoreca),
        },
      },
      /** Reduce el peso del objeto solo dejan do el ID y el nombre */
      {
        $project: {
          punto_id: "$_id",
          punto_nombre: "$nombre",
        },
      },
      /** Busca todos los distribuidores asociados al punto */
      {
        $lookup: {
          from: "distribuidores_vinculados", //Nombre de la colecccion a relacionar
          localField: "punto_id", //Nombre del campo de la coleccion actual
          foreignField: "punto_entrega", //Nombre del campo de la coleccion a relacionar
          pipeline: [
            /** Reduce la data de vinculacion entre punto y distribuidor */
            {
              $project: {
                punto_id: "$punto_entrega",
                distribuidor_id: "$distribuidor",
                estado: "$estado",
                convenio: "$convenio",
                cartera: "$cartera",
                pazysalvo: "$pazysalvo",
                vendedor: "$vendedor",
              },
            },
            /** Equipo comercial asignado */
            {
              $lookup: {
                from: "trabajadors",
                localField: "vendedor", //Nombre del campo de la coleccion actual
                foreignField: "_id", //Nombre del campo de la coleccion a relacionar
                pipeline: [
                  {
                    $project: {
                      vendedor_nombre: {
                        $concat: ["$nombres", " ", "$apellidos"],
                      },
                    },
                  },
                ],
                as: "data_vendedores", //Nombre del campo donde se insertara todos los documentos relacionados
              },
            },
            /** Arma la Info general de cada distribuidor */
            {
              $lookup: {
                from: "distribuidors",
                localField: "distribuidor_id", //Nombre del campo de la coleccion actual
                foreignField: "_id", //Nombre del campo de la coleccion a relacionar
                pipeline: [
                  {
                    $project: {
                      distribuidor_id: "$_id",
                      distribuidor_nombre: "$nombre",
                      distribuidor_pais: "$pais",
                      distribuidor_departamento: "$departamento",
                      distribuidor_ciudad: "$ciudad",
                      distribuidor_nit_cc: "$nit_cc",
                      distribuidor_tiempo_entrega: "$tiempo_entrega",
                      distribuidor_tipo: "$tipo",
                      distribuidor_valor_minimo_pedido: "$valor_minimo_pedido",
                      distribuidor_razon_social: "$razon_social",
                      distribuidor_tipo_persona: "$tipo_persona",
                    },
                  },
                ],
                as: "data_distribuidor", //Nombre del campo donde se insertara todos los documentos relacionados
              },
            },
            /** Busca el totl  pedidos que se hayan entregado entre dist y punto  */
            {
              $lookup: {
                from: "reportepedidos",
                let: {
                  first: "$punto_id",
                  second: "$distribuidor_id",
                },
                pipeline: [
                  /** Retorna solo las compras entre punto y distribuidor */
                  {
                    $match: {
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
                  /** Agrupa los objetos por pedidos ((evita que se cuente el mismo pedido varias veces)) */
                  {
                    $group: {
                      _id: "$idPedido",
                    },
                  },
                  /** Buscamos el estado actual del pedido */
                  {
                    $lookup: {
                      from: "pedidos",
                      localField: "_id", //Nombre del campo de la coleccion actual
                      foreignField: "_id", //Nombre del campo de la coleccion a relacionar
  
                      as: "data_pedido", //Nombre del campo donde se insertara todos los documentos relacionados
                    },
                  },
                  /** Filtra y solo deja de entregado hacia adelante (Comentado en tu código original) */
                  // {
                  //   $match: { "data_pedido.estado": { $in: ["entregado", "finalizado", ...] } }
                  // },
                  /** Crea un estado falso para agruparlos por este campo y contar la cantidad de pedidos */
                  {
                    $project: {
                      pedido_calificacion: {
                        $arrayElemAt: ["$data_pedido.calificacion", 0],
                      },
                      pedido_estado: {
                        $arrayElemAt: ["$data_pedido.estado", 0],
                      },
                      estado_vacio: "vacio",
                    },
                  },
                  {
                    $group: {
                      _id: "$estado_vacio",
                      pedidos_total: { $sum: 1 },
                      /** Agrega todos los pedidos entre el punto y el distribuidor para buscar las cailicaciones */
                      pedido_calificacion: { $push: "$$ROOT" },
                    },
                  },
                ],
                as: "data_compras",
              },
            },
          ],
          as: "data_tabla", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
      /** Desestructura el array data_tabla para obtener un documento por cada distribuidor vinculado */
      {
        $unwind: "$data_tabla",
      },
      {
        $project: {
          punto_nombre: 1,
          vinculacion_estado: "$data_tabla.estado",
          vinculacion_convenio: "$data_tabla.convenio",
          vinculacion_cartera: "$data_tabla.cartera",
          vinculacion_pazysalvo: "$data_tabla.pazysalvo",
          pedidos_entregados_total: {
            $arrayElemAt: ["$data_tabla.data_compras.pedidos_total", 0],
          },
          pedido_todos_calificados: {
            $arrayElemAt: ["$data_tabla.data_compras", 0],
          },
          vendedor_nombre: {
            $arrayElemAt: ["$data_tabla.data_vendedores.vendedor_nombre", 0],
          },
          distribuidor_id: {
            $arrayElemAt: ["$data_tabla.data_distribuidor.distribuidor_id", 0],
          },
          distribuidor_nombre: {
            $arrayElemAt: ["$data_tabla.data_distribuidor.distribuidor_nombre", 0],
          },
          distribuidor_departamento: {
            $arrayElemAt: [
              "$data_tabla.data_distribuidor.distribuidor_departamento",
              0,
            ],
          },
          distribuidor_ciudad: {
            $arrayElemAt: ["$data_tabla.data_distribuidor.distribuidor_ciudad", 0],
          },
          distribuidor_nit_cc: {
            $arrayElemAt: ["$data_tabla.data_distribuidor.distribuidor_nit_cc", 0],
          },
          distribuidor_tiempo_entrega: {
            $arrayElemAt: [
              "$data_tabla.data_distribuidor.distribuidor_tiempo_entrega",
              0,
            ],
          },
          distribuidor_tipo: {
            $arrayElemAt: ["$data_tabla.data_distribuidor.distribuidor_tipo", 0],
          },
          distribuidor_valor_minimo_pedido: {
            $arrayElemAt: [
              "$data_tabla.data_distribuidor.distribuidor_valor_minimo_pedido",
              0,
            ],
          },
          distribuidor_razon_social: {
            $arrayElemAt: [
              "$data_tabla.data_distribuidor.distribuidor_razon_social",
              0,
            ],
          },
          distribuidor_tipo_persona: {
            $arrayElemAt: [
              "$data_tabla.data_distribuidor.distribuidor_tipo_persona",
              0,
            ],
          },
        },
      },
      /** Se agrega este campo para un sort no-case-sensitive */
      {
        $addFields: {
          label_to_sort: {
            $toLower: "$distribuidor_nombre",
          },
          label_to_sort_2: {
            $toLower: "$punto_nombre",
          },
        },
      },
      {
        $sort: { label_to_sort: 1, label_to_sort_2: 1 },
      },
    ]).exec(callback);
  },
  /** Tabla información de los puntos de entrega de un horeca */
  getInformeHorecaTablaPuntosEntrega: function (query, callback) {
    const d = new Date();
    const fecha_referencia = d.setMonth(d.getMonth() - 3);
    this.aggregate([
      /** Recupera los puntos asociados al horeca */
      {
        $match: {
          usuario_horeca: new ObjectId(query.idHoreca),
        },
      },
      /** Reduce el peso del objeto solo dejan do el ID y el nombre */
      {
        $project: {
          punto_id: "$_id",
          punto_nombre: "$nombre",
          punto_pais: "Colombia",
          punto_departamento: "$departamento",
          punto_ciudad: "$ciudad",
          punto_direccion: "$direccion",
          punto_informacion_contacto: "$informacion_contacto",
          punto_sillas: "$sillas",
          punto_domicilios: "$domicilios",
        },
      },
      {
        $lookup: {
          from: "trabajadors",
          localField: "punto_id", //Nombre del campo de la coleccion actual
          foreignField: "puntos_entrega", //Nombre del campo de la coleccion a relacionar
          pipeline: [
            {
              $project: {
                pedido_id: "$_id",
                estado_vacio: "vacio",
              },
            },
            {
              $group: {
                _id: "$estado_vacio",
                total: { $sum: 1 },
              },
            },
          ],
          as: "punto_total_trabajadores", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
      {
        $lookup: {
          from: "distribuidores_vinculados",
          localField: "punto_id", //Nombre del campo de la coleccion actual
          foreignField: "punto_entrega", //Nombre del campo de la coleccion a relacionar
          pipeline: [
            {
              $match: {
                estado: "Aprobado",
              },
            },
            {
              $project: {
                pedido_id: "$_id",
                estado_vacio: "vacio",
              },
            },
            {
              $group: {
                _id: "$estado_vacio",
                total: { $sum: 1 },
              },
            },
          ],
          as: "punto_total_dist_aprobados", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
      {
        $lookup: {
          from: "reportepedidos",
          localField: "punto_id" /** Nombre del campo de la coleccion actual */,
          foreignField:
            "idPunto" /** Nombre del campo de la coleccion a relacionar */,
          pipeline: [
            {
              $match: {
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
                _id: "$idPunto",
                total: { $sum: "$costoTotalProducto" },
              },
            },
          ],
          as: "punto_total_venta_tres_meses" /** Nombre del campo donde se insertara todos los documentos relacionados */,
        },
      },
      /** Se agrega este campo para un sort no-case-sensitive */
      {
        $addFields: {
          label_to_sort: {
            $toLower: "$punto_nombre",
          },
        },
      },
      {
        $sort: { label_to_sort: 1 },
      },
    ]).exec(callback);
  },
  /** Tabla con data de todos los puntos de entrega del horeca */
  getInformeHorecaTablaPuntosFeat: function (query, callback) {
    PedidoBD.aggregate([
      {
        $match: {
          usuario_horeca: new ObjectId(query.idHoreca),
        },
      },
      {
        $lookup: {
          from: 'reportepedidos',
          localField: '_id',
          foreignField: 'idPedido',
          pipeline: [
            {
              $match: {
                puntos_ft_unidad: { $gt: 0 }
              }
            }
          ],
          as: 'productosPedido'
        }
      },
      {
        $unwind: '$productosPedido'
      },
      {
        $addFields: {
          fecha_cierre_puntosft: "$productosPedido.detalleProducto.fecha_cierre_puntosft",
          fecha_apertura_puntosft:  "$productosPedido.detalleProducto.fecha_apertura_puntosft",
        },
      },
      {
        $lookup: {
          from: 'categoria_productos',
          localField: 'productosPedido.categoriaProducto',
          foreignField: '_id',
          pipeline: [
            {
              $project: {
                nombre_categoria: '$nombre'
              }
            }
          ],
          as: "dataCategoria"
        }
      },
      {
        $lookup: {
          from: 'linea_productos',
          localField: 'productosPedido.lineaProducto',
          foreignField: '_id',
          pipeline: [
            {
              $project: {
                nombre_linea: '$nombre'
              }
            }
          ],
          as: 'dataLinea'
        }
      },
      {
        $lookup: {
          from: 'organizacions',
          localField: 'productosPedido.idOrganizacion',
          foreignField: '_id',
          pipeline: [
            {
              $project: {
                nombre_organizacion: '$nombre'
              }
            }
          ],
          as: 'dataOrganizacion'
        }
      },
      {
        $lookup: {
          from: 'distribuidors',
          localField: 'productosPedido.idDistribuidor',
          foreignField: '_id',
          pipeline: [
            {
              $project: {
                nombre_distribuidor: '$nombre',
                nit: '$nit_cc'
              }
            }
          ],
          as: 'dataDistribuidor'
        }
      },
      {
        $lookup: {
          from: 'punto_entregas',
          localField: 'productosPedido.idPunto',
          foreignField: '_id',
          pipeline: [
            {
              $project: {
                nombre_punto: '$nombre'
              }
            }
          ],
          as: 'dataPunto'
        }
      },
      {
        $lookup: {
          from: 'marca_productos',
          localField: 'productosPedido.marcaProducto',
          foreignField: '_id',
          pipeline: [
            {
              $project: {
                nombre_marca: '$nombre'
              }
            }
          ],
          as: 'dataMarca'
        }
      },
      {
        $addFields: {
          puntosFt: {
            $multiply: [
              "$productosPedido.puntos_ft_unidad",
              "$productosPedido.unidadesCompradas"
            ]
          }
        }
      },
      {
        $addFields: {
          valorTotal: {
            $multiply: [
              "$productosPedido.costoProductos",
              "$productosPedido.unidadesCompradas"
            ]
          }
        }
      },
      {
        $project: {
          punto_entrega: { $arrayElemAt: ["$dataPunto.nombre_punto", 0] },
          distribuidor: { $arrayElemAt: ["$dataDistribuidor.nombre_distribuidor", 0] },
          nit: { $arrayElemAt: ["$dataDistribuidor.nit", 0] },
          fecha_pedido: "$fecha",
          pedido_id: "$id_pedido",
          estado_pedido: "$estado",
          categoria: { $arrayElemAt: ["$dataCategoria.nombre_categoria", 0] },
          linea: { $arrayElemAt: ["$dataLinea.nombre_linea", 0] },
          organizacion: { $arrayElemAt: ["$dataOrganizacion.nombre_organizacion", 0] },
          marca: { $arrayElemAt: ["$dataMarca.nombre_marca", 0] },
          codigo_sku: "$productosPedido.detalleProducto.codigo_distribuidor_producto",
          nombre_producto: "$productosPedido.detalleProducto.nombre",
          precio_producto: "$productosPedido.costoProductos",
          puntos_ft_unidad: "$productosPedido.puntos_ft_unidad",
          unidades_pedidas: "$productosPedido.unidadesCompradas",
          valor_total: "$valorTotal",
          puntos_ft_acumulados: "$puntosFt"
        }
      }
    ]).exec(callback);
  },
  /** Tabla con data de todos los pedidos por puntos de entrega del horeca */
  getInformeHorecaTablaPuntosPedidos: function (query, callback) {
    this.aggregate([
      /** Recupera los puntos asociados al horeca */
      {
        $match: {
          usuario_horeca: new ObjectId(query.idHoreca),
        },
      },
      /** Reduce el peso del objeto solo dejan do el ID y el nombre */
      {
        $project: {
          punto_id: "$_id",
          punto_nombre: "$nombre",
        },
      },
      /** Busca todos los productos vendidos del punto */
      {
        $lookup: {
          from: "reportepedidos",
          localField: "punto_id", //Nombre del campo de la coleccion actual
          foreignField: "idPunto", //Nombre del campo de la coleccion a relacionar
          pipeline: [
            /** Calcula el costo del producto segun sus unidades en cada pedido */
            {
              $addFields: {
                pedido_costo: {
                  $multiply: ["$unidadesCompradas", "$costoProductos"],
                },
              },
            },
            /** En el objeto de reporte pedido no tenemos el id de la linea por lo que se agrega */
            /*{
              $addFields: {
                producto_linea_id: {
                  $toObjectId: {
                    $arrayElemAt: ["$lineaProducto", 0],
                  },
                },
              },
            },*/
            /** Organiza la información para aligerar la consulta */
            {
              $project: {
                punto_id: "$idPunto",
                pedido_nombre: "$nombre",
                pedido_unidades_compradas: "$unidadesCompradas",
                pedido_id: "$idPedido",
                pedido_costo: "$pedido_costo",
                pedido_puntosFT_unidad: "$puntos_ft_unidad",
                producto_precios: "$detalleProducto.precios",
                producto_nombre: "$detalleProducto.nombre",
                producto_codigo_distribuidor:
                  "$detalleProducto.codigo_distribuidor_producto",
                producto_codigo_organizacion:
                  "$detalleProducto.codigo_organizacion_producto",
                producto_distribuidor_id: "$idDistribuidor",
                producto_organizacion_id: "$idOrganizacion",
                producto_marca_id: "$marcaProducto",
                producto_categoria_id: "$categoriaProducto",
                producto_linea_id: "$lineaProducto",
              },
            },
            //Nombre distribuidor
            {
              $lookup: {
                from: "distribuidors",
                localField: "producto_distribuidor_id", //Nombre del campo de la coleccion actual
                foreignField: "_id", //Nombre del campo de la coleccion a relacionar
                pipeline: [
                  {
                    $project: {
                      producto_distribuidor_nombre: "$nombre",
                      producto_distribuidor_nit_cc: "$nit_cc",
                    },
                  },
                ],
                as: "data_distribuidor", //Nombre del campo donde se insertara todos los documentos relacionados
              },
            },
            // Nombre organización
            {
              $lookup: {
                from: "organizacions", //Nombre de la colecccion a relacionar
                localField: "producto_organizacion_id", //Nombre del campo de la coleccion actual
                foreignField: "_id", //Nombre del campo de la coleccion a relacionar
                pipeline: [
                  {
                    $project: {
                      producto_organizacion_nombre: "$nombre",
                    },
                  },
                ],
                as: "organizacion_producto", //Nombre del campo donde se insertara todos los documentos relacionados
              },
            },
            // Nombre marca producto
            {
              $lookup: {
                from: "marca_productos", //Nombre de la colecccion a relacionar
                localField: "producto_marca_id", //Nombre del campo de la coleccion actual
                foreignField: "_id", //Nombre del campo de la coleccion a relacionar
                pipeline: [
                  {
                    $project: {
                      producto_marca_nombre: "$nombre",
                    },
                  },
                ],
                as: "marca_producto", //Nombre del campo donde se insertara todos los documentos relacionados
              },
            },
            //Nombre categoria producto
            {
              $lookup: {
                from: "categoria_productos", //Nombre de la colecccion a relacionar
                localField: "producto_categoria_id", //Nombre del campo de la coleccion actual
                foreignField: "_id", //Nombre del campo de la coleccion a relacionar
                pipeline: [
                  {
                    $project: {
                      producto_categoria_nombre: "$nombre",
                    },
                  },
                ],
                as: "categoria_producto", //Nombre del campo donde se insertara todos los documentos relacionados
              },
            },
            //Nombre linea producto
            {
              $lookup: {
                from: "linea_productos", //Nombre de la colecccion a relacionar
                localField: "producto_linea_id", //Nombre del campo de la coleccion actual
                foreignField: "_id", //Nombre del campo de la coleccion a relacionar
                pipeline: [
                  {
                    $project: {
                      producto_linea_nombre: "$nombre",
                    },
                  },
                ],
                as: "linea_producto", //Nombre del campo donde se insertara todos los documentos relacionados
              },
            },
            //Fecha entrega pedido
            {
              $lookup: {
                from: "pedido_trackings", //Nombre de la colecccion a relacionar
                localField: "pedido_id", //Nombre del campo de la coleccion actual
                foreignField: "pedido", //Nombre del campo de la coleccion a relacionar
                pipeline: [
                  //Filtra y solo deja de entregado hacia adelante
                  {
                    $match: {
                      estado_nuevo: "Entregado",
                    },
                  },
                  {
                    $project: {
                      estado_nuevo: "$estado_nuevo",
                      createdAt: "$createdAt",
                    },
                  },
                ],
                as: "data_pedido_tracking", //Nombre del campo donde se insertara todos los documentos relacionados
              },
            },
            // Data pedido (estado y creación)
            {
              $lookup: {
                from: "pedidos", //Nombre de la colecccion a relacionar
                localField: "pedido_id", //Nombre del campo de la coleccion actual
                foreignField: "_id", //Nombre del campo de la coleccion a relacionar
                pipeline: [
                  {
                    $project: {
                      pedido_estado_actual: "$estado",
                      pedido_id: "$id_pedido",
                      pedido_fecha: "$fecha",
                    },
                  },
                ],
                as: "data_pedido", //Nombre del campo donde se insertara todos los documentos relacionados
              },
            },
            //Vendedor del dist asingado al punto 
            {
              $lookup: {
                from: "distribuidores_vinculados",
                let: {
                  first: "$punto_id",
                  second: "$producto_distribuidor_id",
                },
                pipeline: [
                  //Retorna solo las compras entre punto y distribuidor
                  {
                    $match: {
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
                    },
                  },
                  {
                    $project: {
                      vinculacion_vendedor: "$vendedor",
                    },
                  },
                  {
                    $lookup: {
                      from: "trabajadors",
                      localField: "vinculacion_vendedor", //Nombre del campo de la coleccion actual
                      foreignField: "_id", //Nombre del campo de la coleccion a relacionar
                      pipeline: [
                        {
                          $project: {
                            vendedor_nombre: {
                              $concat: ["$nombres", " ", "$apellidos"],
                            },
                          },
                        },
                      ],
                      as: "data_vendedores", //Nombre del campo donde se insertara todos los documentos relacionados
                    },
                  },
                  {
                    $project: {
                      vendedor_nombre: "$data_vendedores.vendedor_nombre",
                    },
                  },
                ],
                as: "data_vinculacion", //Nombre del campo donde se insertara todos los documentos relacionados
              },
            },
            //Información respuesta de la consulta
            {
              $project: {
                punto_nombre: "$punto_nombre",
                pedido_nombre: "$pedido_nombre",
                pedido_unidades_compradas: "$pedido_unidades_compradas",
                pedido_costo: "$pedido_costo",
                pedido_puntosFT_unidad: "$pedido_puntosFT_unidad",
                pedido_estado_actual: {
                  $arrayElemAt: ["$data_pedido.pedido_estado_actual", 0],
                },
                pedido_id: {
                  $arrayElemAt: ["$data_pedido.pedido_id", 0],
                },
                pedido_fecha: {
                  $arrayElemAt: ["$data_pedido.pedido_fecha", 0],
                },
                pedido_fecha_entregado: {
                  $arrayElemAt: ["$data_pedido_tracking.createdAt", 0],
                },
                producto_precios: "$producto_precios",
                producto_nombre: "$producto_nombre",
                producto_codigo_distribuidor: "$producto_codigo_distribuidor",
                producto_codigo_organizacion: "$producto_codigo_organizacion",
                producto_distribuidor_nombre: {
                  $arrayElemAt: [
                    "$data_distribuidor.producto_distribuidor_nombre",
                    0,
                  ],
                },
                producto_distribuidor_nit_cc: {
                  $arrayElemAt: [
                    "$data_distribuidor.producto_distribuidor_nit_cc",
                    0,
                  ],
                },
                producto_organizacion_nombre: {
                  $arrayElemAt: [
                    "$organizacion_producto.producto_organizacion_nombre",
                    0,
                  ],
                },
                producto_marca_nombre: {
                  $arrayElemAt: ["$marca_producto.producto_marca_nombre", 0],
                },
                producto_categoria_nombre: {
                  $arrayElemAt: [
                    "$categoria_producto.producto_categoria_nombre",
                    0,
                  ],
                },
                producto_linea_nombre: {
                  $arrayElemAt: ["$linea_producto.producto_linea_nombre", 0],
                },
                vendedores_asignados: {
                  $arrayElemAt: ["$data_vinculacion.vendedor_nombre", 0],
                },
              },
            },
          ],
          as: "data_pedidos", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
      //Agrega al objeto el nombre del punto 
      {
        $addFields: {
          "data_pedidos.punto_nombre": "$punto_nombre",
        },
      },
      //Deja como raiz el objeto armado omitiendo la demas informacion que no necesitamos
      {
        $unwind: "$data_pedidos",
      },
      {
        $replaceRoot: {
          newRoot: "$data_pedidos",
        },
      },
      //Se agrega este campo para un sort no-case-sensitive de dos campos
      {
        $addFields: {
          label_to_sort: {
            $toLower: "$punto_nombre",
          },
          label_to_sort_2: {
            $toLower: "$pedido_id",
          },
        },
      },
      {
        $sort: { label_to_sort: 1, label_to_sort_2: 1 },
      },
    ]).exec(callback);
  },
  /** Tabla con data los productos vendidos por un tiempo definido */
  getInformeBarraHorecaPedidosProductos: function (query, callback) {
    const query_estado_pedidos = {
      $or: [
        { estado_ped: "Entregado" },
        { estado_ped: "Recibido" },
        { estado_ped: "Calificado" },
      ],
    };
    this.aggregate([
      /** Recupera los puntos asociados al horeca */
      {
        $match: {
          usuario_horeca: new ObjectId(query.idHoreca),
        },
      },
      /** Se buscan las compras por cada punto */
      {
        $lookup: {
          from: "reportepedidos",
          localField: "_id", //Nombre del campo de la coleccion actual
          foreignField: "idPunto", //Nombre del campo de la coleccion a relacionar
          pipeline: [
            /** Agrega la fecha del pedido, la que está en el objeto original está errada*/
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
                from: "pedidos",
                localField: "idPedido",
                foreignField: "_id",
                as: "data_pedido",
              },
            },
            {
              $addFields: {
                fecha_pedido: { $arrayElemAt: ["$data_pedido.fecha", 0] },
                estado_ped: { $arrayElemAt: ["$data_pedido.estado", 0] },
                costo_compra: {
                  $multiply: ["$unidadesCompradas", "$costoProductos"],
                },
              },
            },
            /** Filtra por rango de fecha */
            {
              $match: {
                fecha_pedido: {
                  $gte: new Date(query.inicio),
                  $lte: new Date(query.fin),
                },
              },
            },
           
            /** Agrupa la data por id producto */
            {
              $group: {
                _id: "$productoId",
                total: { $sum: "$costo_compra" },
                producto_cod_feat: {
                  $first: "$codigoFeatProducto",
                },
                producto_nombre: {
                  $first: "$nombreProducto",
                },
              },
            },
          ],
          as: "data_compra",
        },
      },
      {
        $unwind: "$data_compra",
      },
      {
        $replaceRoot: {
          newRoot: "$data_compra",
        },
      },
      {
        $sort: { total: -1 },
      },
    ]).exec(callback);
  },
  /** Tabla con data los productos vendidos por categoria y un tiempo definido */
  getInformeBarraHorecaPedidosCategorias: function (query, callback) {
    this.aggregate([
      /** Recupera los puntos asociados al horeca */
      {
        $match: {
          usuario_horeca: new ObjectId(query.idHoreca),
        },
      },
      /** Se buscan las compras por cada punto */
      {
        $lookup: {
          from: "reportepedidos",
          localField: "_id", //Nombre del campo de la coleccion actual
          foreignField: "idPunto", //Nombre del campo de la coleccion a relacionar
          pipeline: [
            {
              $match: {
                $or: [
                  { "estaActTraking": "Entregado" },
                  { "estaActTraking": "Recibido" },
                  { "estaActTraking": "Calificado" },
                ],
              },
            },
            /** Recupera el nombre de la categoria */
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
            /** Agrega la fecha del pedido, la que está en el objeto original está errada*/
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
                categoria_nombre: {
                  $arrayElemAt: ["$data_categoria.categoria_nombre", 0],
                },
                fecha_pedido: { $arrayElemAt: ["$data_pedido.fecha", 0] },
                costo_compra: {
                  $multiply: ["$unidadesCompradas", "$costoProductos"],
                },
              },
            },
            /** Filtra por rango de fecha */
            {
              $match: {
                fecha_pedido: {
                  $gte: new Date(query.inicio),
                  $lte: new Date(query.fin),
                },
              },
            },
            /** Agrupa la data por id producto */
            {
              $group: {
                _id: "$categoriaProducto",
                total: { $sum: "$costo_compra" },
                categoria_nombre: {
                  $first: "$categoria_nombre",
                },
              },
            },
          ],
          as: "data_compra",
        },
      },
      {
        $unwind: "$data_compra",
      },
      {
        $replaceRoot: {
          newRoot: "$data_compra",
        },
      },
      {
        $sort: { total: -1 },
      },
    ]).exec(callback);
  },
  /** Tabla con data de ventas por distribuidor */
  getInformeBarraHorecaPedidosVentasDistribuidor: function (query, callback) {
    const query_estado_pedidos = {
      $or: [
        { estado_ped: "Entregado" },
        { estado_ped: "Recibido" },
        { estado_ped: "Calificado" },
      ],
    };
    this.aggregate([
      /** Recupera los puntos asociados al horeca */
      {
        $match: {
          usuario_horeca: new ObjectId(query.idHoreca),
        },
      },
      /** Se buscan las compras por cada punto */
      {
        $lookup: {
          from: "reportepedidos",
          localField: "_id", //Nombre del campo de la coleccion actual
          foreignField: "idPunto", //Nombre del campo de la coleccion a relacionar
          pipeline: [
            /** Recupera el nombre del distribuidor */
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
            /** Agrega la fecha del pedido, la que está en el objeto original está errada*/
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
                distribuidor_nombre: {
                  $arrayElemAt: ["$data_distribuidor.distribuidor_nombre", 0],
                },
                fecha_pedido: { $arrayElemAt: ["$data_pedido.fecha", 0] },
                estado_ped: { $arrayElemAt: ["$data_pedido.estado", 0] },
                costo_compra: {
                  $multiply: ["$unidadesCompradas", "$costoProductos"],
                },
              },
            },
            /** Filtra por rango de fecha */
            {
              $match: query_estado_pedidos,
            },
            {
              $match: {
                fecha_pedido: {
                  $gte: new Date(query.inicio),
                  $lte: new Date(query.fin),
                },
              },
            },
            /** Agrupa la data por id producto */
            {
              $group: {
                _id: "$idDistribuidor",
                total: { $sum: "$costo_compra" },
                distribuidor_nombre: {
                  $first: "$distribuidor_nombre",
                },
              },
            },
          ],
          as: "data_compra",
        },
      },
      {
        $unwind: "$data_compra",
      },
      {
        $replaceRoot: {
          newRoot: "$data_compra",
        },
      },
      {
        $sort: { total: -1 },
      },
    ]).exec(callback);
  },
  /** Tabla con data de ventas por distribuidor */
  getInformeBarraHorecaPedidosOrganizacion: function (query, callback) {
     const query_estado_pedidos = {
      $or: [
        { estado_ped: "Entregado" },
        { estado_ped: "Recibido" },
        { estado_ped: "Calificado" },
      ],
    };
    this.aggregate([
      /** Recupera los puntos asociados al horeca */
      {
        $match: {
          usuario_horeca: new ObjectId(query.idHoreca),
        },
      },
      /** Se buscan las compras por cada punto */
      {
        $lookup: {
          from: "reportepedidos",
          localField: "_id", //Nombre del campo de la coleccion actual
          foreignField: "idPunto", //Nombre del campo de la coleccion a relacionar
          pipeline: [
            /** Recupera el nombre de la organizacion */
            {
              $lookup: {
                from: "organizacions",
                localField: "idOrganizacion",
                foreignField: "_id",
                pipeline: [
                  {
                    $project: {
                      organizacion_nombre: "$nombre",
                    },
                  },
                ],
                as: "data_organizacion",
              },
            },
            /** Agrega la fecha del pedido, la que está en el objeto original está errada*/
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
                organizacion_nombre: {
                  $arrayElemAt: ["$data_organizacion.organizacion_nombre", 0],
                },
                estado_ped: { $arrayElemAt: ["$data_pedido.estado", 0] },
                fecha_pedido: { $arrayElemAt: ["$data_pedido.fecha", 0] },
                costo_compra: {
                  $multiply: ["$unidadesCompradas", "$costoProductos"],
                },
              },
            },
            /** Filtra por rango de fecha */
            {
              $match: query_estado_pedidos,
            },
            {
              $match: {
                fecha_pedido: {
                  $gte: new Date(query.inicio),
                  $lte: new Date(query.fin),
                },
              },
            },
            /** Agrupa la data por id producto */
            {
              $group: {
                _id: "$idOrganizacion",
                total: { $sum: "$costo_compra" },
                organizacion_nombre: {
                  $first: "$organizacion_nombre",
                },
              },
            },
          ],
          as: "data_compra",
        },
      },
      {
        $unwind: "$data_compra",
      },
      {
        $replaceRoot: {
          newRoot: "$data_compra",
        },
      },
      {
        $sort: { total: -1 },
      },
    ]).exec(callback);
  },
  /** Tabla con data para informe de tiempo de entrega */
  getInformeTablaHorecaTiempoEntregan: function (query, callback) {
    const query_estado_pedidos = {
      $or: [
        { estado_nuevo: "Entregado" },
        { estado_nuevo: "Recibido" },
        { estado_nuevo: "Calificado" },
      ],
    };
    this.aggregate([
      /** Recupera los puntos asociados al horeca */
      {
        $match: {
          usuario_horeca: new ObjectId(query.idHoreca),
        },
      },
      /** Se buscan las compras por cada punto */
      {
        $lookup: {
          from: "pedidos",
          localField: "_id", //Nombre del campo de la coleccion actual
          foreignField: "punto_entrega", //Nombre del campo de la coleccion a relacionar
          pipeline: [
            /** Recupera el nombre del distribuidor */
            {
              $lookup: {
                from: "distribuidors",
                localField: "distribuidor",
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
            /** Fecha Aprobación pedido */
            {
              $lookup: {
                from: "pedido_trackings", //Nombre de la colecccion a relacionar
                localField: "_id", //Nombre del campo de la coleccion actual
                foreignField: "pedido", //Nombre del campo de la coleccion a relacionar
                pipeline: [
                  {
                    $match: {
                      estado_nuevo: "Aprobado Externo",
                    },
                  },
                  {
                    $project: {
                      estado_nuevo: "$estado_nuevo",
                      createdAt: "$createdAt",
                    },
                  },
                ],
                as: "data_pedido_aprobado_tracking", //Nombre del campo donde se insertara todos los documentos relacionados
              },
            },
            /** Fecha entrega pedido */
            {
              $lookup: {
                from: "pedido_trackings", //Nombre de la colecccion a relacionar
                localField: "_id", //Nombre del campo de la coleccion actual
                foreignField: "pedido", //Nombre del campo de la coleccion a relacionar
                pipeline: [
                  {
                    $match: query_estado_pedidos,
                  },
                  {
                    $project: {
                      estado_nuevo: "$estado_nuevo",
                      createdAt: "$createdAt",
                    },
                  },
                ],
                as: "data_pedido_tracking", //Nombre del campo donde se insertara todos los documentos relacionados
              },
            },
            {
              $project: {
                distribuidor_nombre: {
                  $arrayElemAt: ["$data_distribuidor.distribuidor_nombre", 0],
                },
                fecha_pedido_entregado: {
                  $arrayElemAt: ["$data_pedido_tracking.createdAt", 0],
                },
                fecha_pedido_aprobado: {
                  $arrayElemAt: ["$data_pedido_aprobado_tracking.createdAt", 0],
                },
                fecha_pedido: "$fecha",
              },
            },
            {
              $match: {
                fecha_pedido_entregado: { $ne: null },
                fecha_pedido_aprobado: { $ne: null },
              },
            },
            {
              $addFields: {
                diff: {
                  $subtract: [
                    { $toDate: "$fecha_pedido_entregado" },
                    { $toDate: "$fecha_pedido_aprobado" },
                  ],
                },
              },
            },
          ],
          as: "data_compra",
        },
      },
      {
        $unwind: "$data_compra",
      },
      {
        $replaceRoot: {
          newRoot: "$data_compra",
        },
      },
      {
        $sort: { fecha_pedido_entregado: -1 },
      },
      {
        $limit: 5,
      },
    ]).exec(callback);
  },
  /** Tabla con data pedidos por punto de entrega */
  getInformeBarPuntoEntregaPedidos: function (query, callback) {
    const d = new Date();
    const fecha_referencia = d.setMonth(d.getMonth() - 3);
    this.aggregate([
      /** Recupera los puntos asociados al horeca */
      {
        $match: {
          usuario_horeca: new ObjectId(query.idHoreca),
        },
      },
      /** Se buscan las compras por cada punto */
      {
        $lookup: {
          from: "pedidos",
          localField: "_id", //Nombre del campo de la coleccion actual
          foreignField: "punto_entrega", //Nombre del campo de la coleccion a relacionar
          pipeline: [
            {
              $match: {
                fecha: {
                  $gte: new Date(fecha_referencia),
                },
              },
            },
          ],
          as: "data_compra",
        },
      },
      {
        $addFields: {
          "data_compra.punto_nombre": "$nombre",
        },
      },
      {
        $unwind: "$data_compra",
      },
      {
        $replaceRoot: {
          newRoot: "$data_compra",
        },
      },
      {
        $group: {
          _id: "$punto_nombre",
          total: { $sum: 1 },
        },
      },
      {
        $sort: { total: -1 },
      },
    ]).exec(callback);
  },
  /** Tabla con cantidad de distribuidores por punto de entrega */
  getInformeBarPuntoEntregaDistribuidores: function (query, callback) {
    this.aggregate([
      /** Recupera los puntos asociados al horeca */
      {
        $match: {
          usuario_horeca: new ObjectId(query.idHoreca),
          estado: "Activo",
        },
      },
      {
        $lookup: {
          from: "distribuidores_vinculados",
          localField: "_id", //Nombre del campo de la coleccion actual
          foreignField: "punto_entrega", //Nombre del campo de la coleccion a relacionar
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
        $addFields: {
          "data_vinculacion.punto_nombre": "$nombre",
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
      {
        $group: {
          _id: "$punto_nombre",
          total: { $sum: 1 },
        },
      },
      {
        $sort: { total: -1 },
      },
    ]).exec(callback);
  },
  /** Tabla con participacion en ventas por punto de entrega */
  getInformeBarPuntoEntregaParticipacion: function (query, callback) {
    const d = new Date();
    const fecha_referencia = d.setMonth(d.getMonth() - 3);
    this.aggregate([
      /** Recupera los puntos asociados al horeca */
      {
        $match: {
          usuario_horeca: new ObjectId(query.idHoreca),
        },
      },
      {
        $lookup: {
          from: "pedidos",
          localField: "_id", //Nombre del campo de la coleccion actual
          foreignField: "punto_entrega", //Nombre del campo de la coleccion a relacionar
          pipeline: [
            {
              $match: {
                fecha: {
                  $gte: new Date(fecha_referencia),
                },
              },
            },
          ],
          as: "data_pedidos",
        },
      },
      {
        $addFields: {
          "data_pedidos.punto_nombre": "$nombre",
        },
      },
      {
        $unwind: "$data_pedidos",
      },
      {
        $replaceRoot: {
          newRoot: "$data_pedidos",
        },
      },
      {
        $group: {
          _id: "$punto_nombre",
          total: { $sum: "$total_pedido" },
        },
      },
      {
        $sort: { total: -1 },
      },
    ]).exec(callback);
  },
  /** Tabla con cantidad de trabajadores por punto de entrega */
  getInformeBarPuntoEntregaTrabajadores: function (query, callback) {
    this.aggregate([
      /** Recupera los puntos asociados al horeca */
      {
        $match: {
          usuario_horeca: new ObjectId(query.idHoreca),
          estado: "Activo",
        },
      },
      {
        $lookup: {
          from: "trabajadors",
          localField: "_id", //Nombre del campo de la coleccion actual
          foreignField: "puntos_entrega", //Nombre del campo de la coleccion a relacionar
          pipeline: [
            {
              $match: {
                solicitud_vinculacion: "Aprobado",
              },
            },
          ],
          as: "data_trabajador",
        },
      },
      {
        $addFields: {
          "data_trabajador.punto_nombre": "$nombre",
        },
      },
      {
        $unwind: "$data_trabajador",
      },
      {
        $replaceRoot: {
          newRoot: "$data_trabajador",
        },
      },
      {
        $group: {
          _id: "$punto_nombre",
          total: { $sum: 1 },
        },
      },
      {
        $sort: { total: -1 },
      },
    ]).exec(callback);
  },
  /** Data de compra por categorias para el detalle de un distribuidor */
  getInformePieDistribuidorCompraCtg: function (query, callback) {
    this.aggregate([
      /** Recupera los puntos asociados al horeca */
      {
        $match: {
          usuario_horeca: new ObjectId(query.idHoreca),
        },
      },
      /** Vendedor del dist asingado al punto  */
      {
        $lookup: {
          from: "reportepedidos",
          let: {
            punto: "$_id",
            distribuidor: new ObjectId(query.idDistribuidor),
          },
          pipeline: [
            /** Retorna solo las compras entre punto y distribuidor */
            {
              $match: {
                $expr: {
                  $and: [
                    {
                      $eq: ["$idPunto", "$$punto"],
                    },
                    {
                      $eq: ["$idDistribuidor", "$$distribuidor"],
                    },
                  ],
                },
              },
            },
          ],
          as: "data_compras", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
      // {
      //   $addFields: {
      //     "data_compras.categoria_producto":
      //       "$data_compras.categoria_producto.nombre",
      //     // pedido_costo: {
      //     //   $multiply: ["$unidadesCompradas", "$costoProductos"],
      //     // },
      //   },
      // },
      {
        $unwind: "$data_compras",
      },
      {
        $replaceRoot: {
          newRoot: "$data_compras",
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
      {
        $addFields: {
          pedido_costo: {
            $multiply: ["$unidadesCompradas", "$costoProductos"],
          },
        },
      },
      {
        $project: {
          pedido_costo: "$pedido_costo",
          categoria_producto: {
            $arrayElemAt: ["$categoria_producto.nombre", 0],
          },
        },
      },
      {
        $group: {
          _id: "$categoria_producto",
          total: { $sum: "$pedido_costo" },
        },
      },
      {
        $match: {
          total: {
            $ne: 0,
          },
        },
      },
      {
        $sort: { total: -1 },
      },
    ]).exec(callback);
  },
  countUsuarios: function (req, callback) {
    this.aggregate([
      {
        $match: {
          createdAt: {
            $gte: new Date(req.inicio),
            $lte: new Date(req.fin),
          },
        },
      },
    ]).exec(callback);
  },
  /** Data de compra por producto para el detalle de un distribuidor */
  getInformePieDistribuidorCompraProductos: function (query, callback) {
    this.aggregate([
      /** Recupera los puntos asociados al horeca */
      {
        $match: {
          usuario_horeca: new ObjectId(query.idHoreca),
        },
      },
      /** Vendedor del dist asingado al punto  */
      {
        $lookup: {
          from: "reportepedidos",
          let: {
            punto: "$_id",
            distribuidor: new ObjectId(query.idDistribuidor),
          },
          pipeline: [
            /** Retorna solo las compras entre punto y distribuidor */
            {
              $match: {
                $expr: {
                  $and: [
                    {
                      $eq: ["$idPunto", "$$punto"],
                    },
                    {
                      $eq: ["$idDistribuidor", "$$distribuidor"],
                    },
                  ],
                },
              },
            },
          ],
          as: "data_compras", //Nombre del campo donde se insertara todos los documentos relacionados
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
          _id: "$nombreProducto",
          unidades: { $sum: "$unidadesCompradas" },
          cajas: { $sum: "$caja" },
          foto: {
            $last: { $arrayElemAt: ["$detalleProducto.fotos", 0] },
          },
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
};

const Punto_entrega = (module.exports = mongoose.model(
  "Punto_entrega",
  Punto_entregaSchema
));

module.exports.getPuntosByUsuarioHoreca = function (obj, callback) {
  const query = { usuario_horeca: obj.usuario_horeca };
  Punto_entrega.find(query, callback);
};

module.exports.getDistincPuntobyField = function (field, callback) {
  Punto_entrega.find().distinct(field).exec(callback);
};
