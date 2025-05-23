const mongoose = require("mongoose");
const bcrypt = require("bcryptjs");
const PuntosBD = require("./punto_entrega_model");
const DistribuidoresBD = require("./distribuidores_vinculados_model");
const PedidosBD = require("./pedido_model");
const PedidosTrakingBD = require("./pedido_tracking_model");
const DistribuidorBD = require("./distribuidor_model");
const TrabajadoresBD = require("./trabajador_model");
const DocumentosBD = require("./documentos_solicitante_model");
var moment = require("moment");

// Schema

const Usuario_horecaSchema = mongoose.Schema({
  nombres: { type: String, required: true },
  apellidos: { type: String, required: true },
  correo: { type: String, required: true },
  pais: { type: String, required: false },
  departamento: { type: String, required: false },
  ciudad: { type: String, required: false },
  tipo_documento: { type: String, required: true },
  numero_documento: { type: String, required: true },
  clave: { type: String, required: true },
  telefono: { type: String, required: false },
  origen: { type: String, required: false },
  celular: { type: String, required: true },
  tipo_aprobador: {
    type: String,
    required: true,
    default: "No Aprobador",
    enum: ["Aprobador", "No Aprobador"],
  },
  nombre_establecimiento: { type: String, required: false },
  empresa_pais: { type: String, required: false },
  empresa_departamento: { type: String, required: false },
  empresa_ciudad: { type: String, required: false },
  empresa_telefono: { type: Number, required: false },
  empresa_telefono2: { type: Number, required: false },
  propietario_tipo_documento: { type: String, required: false },
  propietario_numero_documento: { type: String, required: false },
  propietario_nombres: { type: String, required: false },
  propietario_apellidos: { type: String, required: false },
  propietario_telefono: { type: Number, required: false },
  propietario_correo: { type: String, required: false },
  logo: {
    type: String,
    required: false,
  },
  longitud: { type: Number, required: false, default: 0 },
  latitud: { type: Number, required: false, default: 0 },
  solicitud_vinculacion: {
    type: String,
    required: false,
    default: "Aprobado",
    enum: ["Aprobado", "Rechazado", "Cancelado", "Pendiente", "Creado distribuidor"],
  },
  rol: {
    type: String,
    required: true,
    default: "Administrador",
    enum: ["Horeca", "Distribuidor", "Organizacion", "Administrador"],
  },
  razon_social: { type: String, required: false },
  nit: { type: String, required: false },
  tipo_usuario: {
    type: String,
    required: true,
    default: "Natural",
    enum: ["Jurídica", "Natural"],
  },
  tipo_negocio: {
    type: String,
    required: true,
    default: "RESTAURANTE",
    enum: [
      "BAR / DISCOTECA",
      "CAFETERÍA / HELADERÍA / SNACK",
      "CATERING SERVICE / SERVICIO ALIMENTACIÓN",
      "COCINA OCULTA",
      "CENTRO DE DIVERSIÓN",
      "CENTRO DEPORTIVO Y GIMNASIOS",
      "CLUB SOCIAL / NEGOCIOS",
      "COMEDOR DE EMPLEADOS",
      "COMIDA RÁPIDA",
      "HOGAR",
      "MAYORISTA / MINORISTA",
      "OFICINA / COWORKING",
      "PANADERÍA / REPOSTERÍA",
      "PROPIEDAD HORIZONTAL",
      "RESTAURANTE",
      "RESTAURANTE DE CADENA",
      "SECTOR EDUCACIÓN",
      "SECTOR HOTELERO",
      "SECTOR SALUD",
      "SECTOR PÚBLICO / PRIVADO",
    ],
  },
  createdAt: { type: Date, required: false, default: Date.now },
});

Usuario_horecaSchema.statics = {
  get: function (query, callback) {
    this.findOne(query).exec(callback);
  },
  getAll: function (query, callback) {
    this.find(query).exec(callback);
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
    const Usuario_horeca = new this(data);
    Usuario_horeca.save(callback);
  },
  getAllMailings: function (data, callback) {
    this.aggregate([
      {
        $match: {},
      },
      {
        $lookup: {
          from: "trabajadors", //Nombre de la colecccion a relacionar
          localField: "_id", //Nombre del campo de la coleccion actual
          foreignField: "usuario_horeca", //Nombre del campo de la coleccion a relacionar
          pipeline: [
            {
              $match: {
                tipo_trabajador: "PROPIETARIO/REP LEGAL",
              },
            },
          ],
          as: "representante", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
      {
        $project: {
          tipo_negocio: "$tipo_negocio",
          tipo_usuario: "$tipo_usuario",
          establecimiento: "$nombre_establecimiento",
          tipo_documento: "$tipo_documento",
          nombres01: "$nombres",
          apellidos01: "$apellidos",
          celular: "$celular",
          correo: "$representante.correo",
          nombres02: "$representante.nombres",
          apellidos02: "$representante.apellidos",
          usuarioClave: "$correo",
          estado: "$solicitud_vinculacion",
          fecha: "$createdAt",
        },
      },
      {
        $sort: { fecha: -1 },
      },
    ]).exec(callback);
  },
  get_agrupados: function (query, callback) {
    let final = moment().subtract(3, "M");
    let fecha_actual = new Date();
    let fecha_inicial = new Date(final);
    if (query == "all") {
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
            from: "punto_entregas",
            localField: "_id", //Nombre del campo de la coleccion actual
            foreignField: "usuario_horeca", //Nombre del campo de la coleccion a relacionar
            pipeline: [{ $limit: 1 }],
            as: "Puntos", //Nombre del campo donde se insertara todos los documentos relacionados
          },
        },

        {
          $addFields: {
            newSolicitud: { input: "$Puntos.domicilios", to: "string" },
          },
        },
        {
          $lookup: {
            //from: DistribuidoresBD.collection.name, //Nombre de la colecccion a relacionar
            from: "distribuidors",
            localField: "Puntos._id", //Nombre del campo de la coleccion actual
            foreignField: "punto_entrega", //Nombre del campo de la coleccion a relacionar
            pipeline: [{ $limit: 1 }],
            as: "Distribuidores_vinculados", //Nombre del campo donde se insertara todos los documentos relacionados
          },
        },
        {
          $lookup: {
            //from: PedidosBD.collection.name, //Nombre de la colecccion a relacionar
            from: "pedidos",
            localField: "_id", //Nombre del campo de la coleccion actual
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
          $lookup: {
            from: TrabajadoresBD.collection.name, //Nombre de la colecccion a relacionar
            localField: "_id", //Nombre del campo de la coleccion actual
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
            localField: "_id", //Nombre del campo de la coleccion actual
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
            from: "punto_entregas",
            localField: "user_id", //Nombre del campo de la coleccion actual
            foreignField: "usuario_horeca", //Nombre del campo de la coleccion a relacionar
            as: "Puntos_total", //Nombre del campo donde se insertara todos los documentos relacionados
          },
        },
        {
          $lookup: {
            // from: DistribuidoresBD.collection.name, //Nombre de la colecccion a relacionar
            from: "distribuidors",
            localField: "Puntos_total._id", //Nombre del campo de la coleccion actual
            foreignField: "punto_entrega", //Nombre del campo de la coleccion a relacionar
            as: "Distribuidores_total", //Nombre del campo donde se insertara todos los documentos relacionados
          },
        },
        {
          $project: {
            nombre_establecimiento: "$nombre_establecimiento",
            nombres: { $concat: ["$nombres", " ", "$apellidos"] },
            tipo_negocio: "$tipo_negocio",
            razon_social: "$razon_social",
            tipo_usuario: "$tipo_usuario",
            nit: "$nit",
            correo: "$correo",
            solicitud_vinculacion: "$solicitud_vinculacion",
            telefono: "$celular",
            pais: "$pais",
            departamento: "$departamento",
            ciudad: "$ciudad",
            punto_entrega_nombre: "$Puntos.nombre",
            sillas: "$Puntos.sillas",
            direccionPunto: "$Puntos.direccion",
            example: "$Puntos.domicilios",
            domicilio: {
              $cond: {
                if: { $eq: ["$Puntos.domicilios", "true"] },
                then: "Si",
                else: "No",
              },
            },
            convenio: "$Distribuidores_vinculados.convenio",
            cartera_vencida: "$Distribuidores_vinculados.pazysalvo",
            usuario_admin: "$usuariosAdministradores",
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
            total_administradores: {
              $sum: { $size: "$usuariosAdministradores" },
            },
          },
        },
      ]).exec(callback);
    } else {
      let arreglo = query.split(",");
      let busqueda = [];
      for (let s of arreglo) {
        if (s == "PANADERÍA") {
          busqueda.push({ tipo_negocio: "PANADERÍA / REPOSTERÍA" });
        }
        if (s == "COLEGIO") {
          busqueda.push({ tipo_negocio: "COLEGIO / UNIVERSIDAD / CLUB" });
        }
        if (s == "HOTEL") {
          busqueda.push({ tipo_negocio: "HOTEL / HOSPITAL" });
        }
        if (s == "COMEDOR") {
          busqueda.push({ tipo_negocio: "COMEDOR DE EMPLEADOS" });
        }
        if (s == "CENTRO") {
          busqueda.push({ tipo_negocio: "CENTRO DE DIVERSIÓN" });
        }
        if (s == "CAFETERIA") {
          busqueda.push({ tipo_negocio: "CAFETERÍA / HELADERÍA / SNACK" });
        }
        if (s == "CATERING") {
          busqueda.push({
            tipo_negocio: "CATERING SERVICE / SERVICIO ALIMENTACIÓN",
          });
        }
        if (s == "RESTAURANTECADENA") {
          busqueda.push({ tipo_negocio: "RESTAURANTE DE CADENA" });
        }
        if (s === "BADISCO") {
          busqueda.push({ tipoNegocioUser: "BAR / DISCOTECA" });
        }
        if (s == "COMIDA") {
          busqueda.push({ tipo_negocio: "COMIDA RÁPIDA" });
        }
        if (s == "MAYORISTA") {
          busqueda.push({ tipo_negocio: "MAYORISTA / MINORISTA" });
        } else {
          busqueda.push({ tipo_negocio: s });
        }
      }
      this.aggregate([
        {
          $match: {
            $or: busqueda,
          },
        },
        {
          $lookup: {
            //from: PuntosBD.collection.name, //Nombre de la colecccion a relacionar
            from: "punto_entregas",
            localField: "_id", //Nombre del campo de la coleccion actual
            foreignField: "usuario_horeca", //Nombre del campo de la coleccion a relacionar
            pipeline: [{ $limit: 1 }],
            as: "Puntos", //Nombre del campo donde se insertara todos los documentos relacionados
          },
        },
        {
          $lookup: {
            // from: DistribuidoresBD.collection.name, //Nombre de la colecccion a relacionar
            from: "distribuidors",
            localField: "Puntos._id", //Nombre del campo de la coleccion actual
            foreignField: "punto_entrega", //Nombre del campo de la coleccion a relacionar
            pipeline: [{ $limit: 1 }],
            as: "Distribuidores_vinculados", //Nombre del campo donde se insertara todos los documentos relacionados
          },
        },
        {
          $lookup: {
            //from: PedidosBD.collection.name, //Nombre de la colecccion a relacionar
            from: "pedidos",
            localField: "_id", //Nombre del campo de la coleccion actual
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
          $lookup: {
            from: TrabajadoresBD.collection.name, //Nombre de la colecccion a relacionar
            localField: "_id", //Nombre del campo de la coleccion actual
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
            localField: "_id", //Nombre del campo de la coleccion actual
            foreignField: "usuario_horeca", //Nombre del campo de la coleccion a relacionar
            pipeline: [
              //Filtro de busqueda
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
            from: PuntosBD.collection.name, //Nombre de la colecccion a relacionar
            localField: "_id", //Nombre del campo de la coleccion actual
            foreignField: "usuario_horeca", //Nombre del campo de la coleccion a relacionar
            as: "Puntos_total", //Nombre del campo donde se insertara todos los documentos relacionados
          },
        },
        {
          $lookup: {
            from: DistribuidoresBD.collection.name, //Nombre de la colecccion a relacionar
            localField: "Puntos_total._id", //Nombre del campo de la coleccion actual
            foreignField: "punto_entrega", //Nombre del campo de la coleccion a relacionar
            as: "Distribuidores_total", //Nombre del campo donde se insertara todos los documentos relacionados
          },
        },
        {
          $project: {
            nombre_establecimiento: "$nombre_establecimiento",
            nombres: { $concat: ["$nombres", " ", "$apellidos"] },
            tipo_negocio: "$tipo_negocio",
            razon_social: "$razon_social",
            tipo_usuario: "$tipo_usuario",
            nit: "$nit",
            correo: "$correo",
            telefono: "$celular",
            pais: "$pais",
            departamento: "$departamento",
            ciudad: "$ciudad",
            punto_entrega_nombre: "$Puntos.nombre",
            solicitud_vinculacion: "$solicitud_vinculacion",
            sillas: "$Puntos.sillas",
            direccionPunto: "$Puntos.direccion",
            domicilio: {
              $cond: {
                if: { $eq: ["$Puntos.domicilios", true] },
                then: "Si",
                else: "No",
              },
            },
            convenio: "$Distribuidores_vinculados.convenio",
            cartera_vencida: "$Distribuidores_vinculados.pazysalvo",
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
            total_administradores: {
              $sum: { $size: "$usuariosAdministradores" },
            },
          },
        },
      ]).exec(callback);
    }
  },

  getPuntos_por_establecimientos: function (query, callback) {
    this.aggregate([
      {
        $lookup: {
          //from: PuntosBD.collection.name, //Nombre de la colecccion a relacionar
          from: "punto_entregas", //Nombre de la colecccion a relacionar
          localField: "_id", //Nombre del campo de la coleccion actual
          foreignField: "usuario_horeca", //Nombre del campo de la coleccion a relacionar
          as: "Puntos", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
      {
        $addFields: {
          total_puntos: { $size: "$Puntos" },
        },
      },
      {
        $facet: {
          puntos01: [
            {
              $match: {
                total_puntos: 1,
              },
            },
          ],
          puntos02: [
            {
              $match: {
                total_puntos: 2,
              },
            },
          ],
          puntos03: [
            {
              $match: {
                total_puntos: {
                  $gte: 3,
                  $lte: 5,
                },
              },
            },
          ],
          puntos04: [
            {
              $match: {
                total_puntos: {
                  $gte: 6,
                  $lte: 10,
                },
              },
            },
          ],
          puntos05: [
            {
              $match: {
                total_puntos: {
                  $gte: 11,
                  $lte: 20,
                },
              },
            },
          ],
          puntos06: [
            {
              $match: {
                total_puntos: {
                  $gte: 21,
                },
              },
            },
          ],
        },
      },
      {
        $project: {
          resultado_puntos: [
            {
              label: "1",
              count: { $size: "$puntos01" },
            },
            {
              label: "2",
              count: { $size: "$puntos02" },
            },
            {
              label: "3-5",
              count: { $size: "$puntos03" },
            },
            {
              label: "6-10",
              count: { $size: "$puntos04" },
            },
            {
              label: "11-20",
              count: { $size: "$puntos05" },
            },
            {
              label: "+21",
              count: { $size: "$puntos06" },
            },
          ],
        },
      },
    ]).exec(callback);
  },
  getEstablecimientos_por_ciudad: function (query, callback) {
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
          _id: "$ciudad",
          cantidad: { $sum: 1 },
        },
      },
    ]).exec(callback);
  },
  getEstablecimientos_por_tipo: function (query, callback) {
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
          _id: "$tipo_negocio",
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
  getPuntosMap: function (query, callback) {
    this.aggregate([
      {
        $match: {},
      },
    ]).exec(callback);
  },
  getEstablecimientos_por_tipoPersona: function (query, callback) {
    this.aggregate([
      {
        $lookup: {
          from: "punto_entregas",
          localField: "_id",
          foreignField: "usuario_horeca",
          as: "puntos"
        }
      },
      {
        $unwind: "$puntos"
      },
      {
        $group: {
          _id: "$tipo_usuario",
          cantidad: { $sum: 1 },
        },
      },
    ]).exec(callback);
  },
  indicadoresUser: function (query, callback) {
    var ObjectId = require("mongoose").Types.ObjectId;
    this.aggregate([
      {
        $match: {
          _id: new ObjectId(query),
        },
      },
      {
        $lookup: {
          from: "punto_entregas", //Nombre de la colecccion a relacionar
          localField: "_id", //Nombre del campo de la coleccion actual
          foreignField: "usuario_horeca", //Nombre del campo de la coleccion a relacionar
          as: "Puntos", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
      {
        $lookup: {
          from: "pedidos", //Nombre de la colecccion a relacionar
          localField: "_id", //Nombre del campo de la coleccion actual
          foreignField: "usuario_horeca", //Nombre del campo de la coleccion a relacionar
          as: "pedidosGenerales", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
      {
        $lookup: {
          from: TrabajadoresBD.collection.name, //Nombre de la colecccion a relacionar
          localField: "_id", //Nombre del campo de la coleccion actual
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
          localField: "_id", //Nombre del campo de la coleccion actual
          foreignField: "usuario_horeca", //Nombre del campo de la coleccion a relacionar
          pipeline: [
            //Filtro de busqueda
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
          from: "distribuidores_vinculados", //Nombre de la colecccion a relacionar
          localField: "Puntos._id", //Nombre del campo de la coleccion actual
          foreignField: "punto_entrega", //Nombre del campo de la coleccion a relacionar
          as: "Distribuidores_total", //Nombre del campo donde se insertara todos los documentos relacionados
          pipeline: [
            {
              $match: {
                estado: "Aprobado",
              },
            },
          ],
        },
      },
      {
        $lookup: {
          from: "pedidos", //Nombre de la colecccion a relacionar
          localField: "_id", //Nombre del campo de la coleccion actual
          foreignField: "usuario_horeca", //Nombre del campo de la coleccion a relacionar
          as: "pedidosGenerales", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
      {
        $project: {
          nombres: "$nombre_establecimiento",
          total_puntos: {
            $sum: { $size: "$Puntos" },
          },
          pedidos: {
            $sum: { $size: "$pedidosGenerales" },
          },
          usuarios_admin: {
            $sum: { $size: "$usuariosAdministradores" },
          },
          usuarios_planificadores: {
            $sum: { $size: "$usuariosPlaneadores" },
          },
          distribuidores_vinculados: "$Distribuidores_total",
          /*distribuidores_vinculados: {
            $sum: { $size: "$Distribuidores_total" },
          },*/
          puntos_ganados: {
            $sum: { $sum: "$pedidosGenerales.puntos_ganados" },
          },
          puntos_redimidos: {
            $sum: { $sum: "$pedidosGenerales.puntos_redimidos" },
          },
          sillas: { $sum: "$Puntos.sillas" },
          promedioPedidos: { $sum: "$pedidosGenerales.total_pedido" },
        },
      },
    ]).exec(callback);
  },
  get_dataUser: function (query, callback) {
    var ObjectId = require("mongoose").Types.ObjectId;
    this.aggregate([
      {
        $match: {
          _id: new ObjectId(query),
        },
      },
      {
        $lookup: {
          from: TrabajadoresBD.collection.name, //Nombre de la colecccion a relacionar
          localField: "_id", //Nombre del campo de la coleccion actual
          foreignField: "usuario_horeca", //Nombre del campo de la coleccion a relacionar
          pipeline: [
            {
              $match: {
                tipo_trabajador: "PROPIETARIO/REP LEGAL",
              },
            },
          ],
          as: "representante", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
      {
        $lookup: {
          from: DocumentosBD.collection.name, //Nombre de la colecccion a relacionar
          localField: "_id", //Nombre del campo de la coleccion actual
          foreignField: "usuario", //Nombre del campo de la coleccion a relacionar
          as: "documentacion", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
      {
        $project: {
          dataUser: {
            nombres: "$nombres",
            apellidos: "$apellidos",
            celular: "$celular",
            ciudad: "$ciudad",
            correo: "$correo",
            departamento: "$departamento",
            logo: "$logo",
            nit: "$nit",
            nombre_establecimiento: "$nombre_establecimiento",
            numero_documento: "$numero_documento",
            pais: "$pais",
            razon_social: "$razon_social",
            rol: "$rol",
            solicitud_vinculacion: "$solicitud_vinculacion",
            telefono: "$telefono",
            tipo_aprobador: "$tipo_aprobador",
            tipo_documento: "$tipo_documento",
            tipo_establecimiento: "$tipo_establecimiento",
            tipo_negocio: "$tipo_negocio",
            tipo_usuario: "$tipo_usuario",
            _id: "$_id",
          },
          representantes: "$representante",
          documentos: "$documentacion",
        },
      },
    ]).exec(callback);
  },
  get_dataUserAll: function (query, callback) {
    this.aggregate([
      {
        $match: {},
      },
      {
        $lookup: {
          from: "punto_entregas", //Nombre de la colecccion a relacionar
          localField: "_id", //Nombre del campo de la coleccion actual
          foreignField: "usuario_horeca", //Nombre del campo de la coleccion a relacionar
          as: "puntos", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
      {
        $lookup: {
          from: TrabajadoresBD.collection.name, //Nombre de la colecccion a relacionar
          localField: "_id", //Nombre del campo de la coleccion actual
          foreignField: "usuario_horeca", //Nombre del campo de la coleccion a relacionar
          pipeline: [
            {
              $match: {
                tipo_trabajador: "PROPIETARIO/REP LEGAL",
              },
            },
          ],
          as: "representante", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
      {
        $project: {
          nombres: "$nombres",
          apellidos: "$apellidos",
          celular: "$celular",
          ciudad: "$ciudad",
          correo: "$correo",
          departamento: "$departamento",
          logo: "$logo",
          nit: "$nit",
          nombre_establecimiento: "$nombre_establecimiento",
          numero_documento: "$numero_documento",
          pais: "$pais",
          razon_social: "$razon_social",
          rol: "$rol",
          solicitud_vinculacion: "$solicitud_vinculacion",
          telefono: "$telefono",
          tipo_aprobador: "$tipo_aprobador",
          tipo_documento: "$tipo_documento",
          tipo_establecimiento: "$tipo_establecimiento",
          tipo_negocio: "$tipo_negocio",
          tipo_usuario: "$tipo_usuario",
          fechaCreado: {
            $dateToString: {
              format: "%Y-%m-%d",
              date: "$createdAt",
            },
          },
          _id: "$_id",
          CantPuntos: { $size: "$puntos" },
          representantes: "$representante",
        },
      },
      {
        $match: {
          CantPuntos: 0,
        },
      },
    ]).exec(callback);
  },
  /******************************** Grafica Horeca Informes ********************************/
  /** Tabla con data de los establecimientos por tipo de ngocio */
  getInformeBarraEstablecimientosNegocio: function (query, callback) {
    var ObjectId = require("mongoose").Types.ObjectId;
    this.aggregate([
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
  /** Tabla con data de los establecimientos por tipo de ngocio */
  getInformeBarraEstablecimientosCiudad: function (query, callback) {
    var ObjectId = require("mongoose").Types.ObjectId;
    this.aggregate([
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
  getAllMsj: function (query, callback) {
    this.aggregate([
      {
        $match: {},
      },
      {
        $lookup: {
          from: "punto_entregas", //Nombre de la colecccion a relacionar
          localField: "_id", //Nombre del campo de la coleccion actual
          foreignField: "usuario_horeca", //Nombre del campo de la coleccion a relacionar
          as: "puntos", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
      {
        $lookup: {
          from: TrabajadoresBD.collection.name, //Nombre de la colecccion a relacionar
          localField: "_id", //Nombre del campo de la coleccion actual
          foreignField: "usuario_horeca", //Nombre del campo de la coleccion a relacionar
          pipeline: [
            {
              $match: {
                tipo_trabajador: "PROPIETARIO/REP LEGAL",
              },
            },
          ],
          as: "representante", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
      {
        $project: {
          nombres: "$nombres",
          apellidos: "$apellidos",
          celular: "$celular",
          ciudad: "$ciudad",
          correo: "$correo",
          departamento: "$departamento",
          logo: "$logo",
          nit: "$nit",
          nombre_establecimiento: "$nombre_establecimiento",
          numero_documento: "$numero_documento",
          pais: "$pais",
          razon_social: "$razon_social",
          rol: "$rol",
          solicitud_vinculacion: "$solicitud_vinculacion",
          telefono: "$telefono",
          tipo_aprobador: "$tipo_aprobador",
          tipo_documento: "$tipo_documento",
          tipo_establecimiento: "$tipo_establecimiento",
          tipo_negocio: "$tipo_negocio",
          tipo_usuario: "$tipo_usuario",
          fechaCreado: {
            $dateToString: {
              format: "%Y-%m-%d",
              date: "$createdAt",
            },
          },
          _id: "$_id",
          CantPuntos: { $size: "$puntos" },
          representantes: "$representante",
        },
      },
      {
        $match: {
          CantPuntos: 0,
        },
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
  horecaSinVincular: function (req, callback) {
    this.aggregate([
      {
        $match: {},
      },
      {
        $lookup: {
          from: "punto_entregas", //Nombre de la colecccion a relacionar
          localField: "_id", //Nombre del campo de la coleccion actual
          foreignField: "usuario_horeca", //Nombre del campo de la coleccion a relacionar
          pipeline: [
            {
              $lookup: {
                from: "distribuidores_vinculados", //Nombre de la colecccion a relacionar
                localField: "_id", //Nombre del campo de la coleccion actual
                foreignField: "punto_entrega", //Nombre del campo de la coleccion a relacionar
                as: "data_vinculacion", //Nombre del campo donde se insertara todos los documentos relacionados

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
          as: "data_puntos", //Nombre del campo donde se insertara todos los documentos relacionados
        },

      },
      {
        $addFields: {
          "totalCount": { $size: "$data_puntos" }
        }
      },

      {
        $match: {
          totalCount: 0
        },
      },
    ]).exec(callback);
  },
  // Indicadores Home Organización
  getIndicadoresHomeHoreca: function (query, callback) {
    var ObjId = require("mongoose").Types.ObjectId;
    this.aggregate([
      {
        $match: {
          _id: new ObjId(query._id),
        },
      },
      {
        $addFields: {
          _id_user: new ObjId(query._id_user),
        },
      },
      {
        $facet: {
          mensajes_nuevos: [
            // Puntos asociados al trabajador
            {
              $lookup: {
                from: "trabajadors", //Nombre de la colecccion a relacionar
                localField: "_id_user", //Nombre del campo de la coleccion actual
                foreignField: "_id", //Nombre del campo de la coleccion a relacionar
                pipeline: [
                  {
                    $project: {
                      punto_entrega: "$puntos_entrega",
                    },
                  },
                ],
                as: "data_trabajador", //Nombre del campo donde se insertara todos los documentos relacionados
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
            // Pedidos del punto y filtra por estado
            {
              $lookup: {
                from: "pedidos", //Nombre de la colecccion a relacionar
                localField: "punto_entrega", //Nombre del campo de la coleccion actual
                foreignField: "punto_entrega", //Nombre del campo de la coleccion a relacionar
                pipeline: [
                  {
                    $match: {
                      $or: [
                        { estado: "Aprobado Interno" },
                        { estado: "Aprobado Externo" },
                        { estado: "Alistamiento" },
                        { estado: "Despachado" },
                        { estado: "Facturado" },
                      ],
                    },
                  },
                  {
                    $project: {
                      _id: "$_id",
                      estado: "$estado",
                    },
                  },
                ],
                as: "data_pedidos", //Nombre del campo donde se insertara todos los documentos relacionados
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
              $lookup: {
                from: "mensajes", //Nombre de la colecccion a relacionar
                localField: "_id", //Nombre del campo de la coleccion actual
                foreignField: "pedido", //Nombre del campo de la coleccion a relacionar
                pipeline: [
                  {
                    $project: { ultimo_mensaje: { $last: "$conversacion" } },
                  },
                ],
                as: "data_ultimo_mensaje_room", //Nombre del campo donde se insertara todos los documentos relacionados
              },
            },
            {
              $unwind: "$data_ultimo_mensaje_room",
            },
            {
              $replaceRoot: {
                newRoot: "$data_ultimo_mensaje_room",
              },
            },
            {
              $match: {
                "ultimo_mensaje.leido": false,
              },
            },
            {
              $group: {
                _id: "$ultimo_mensaje.leido",
                total: { $sum: 1 },
              },
            },
          ],
          notificaciones: [
            {
              $lookup: {
                from: "notificaciones", //Nombre de la colecccion a relacionar
                localField: "_id_user", //Nombre del campo de la coleccion actual
                foreignField: "destinatario", //Nombre del campo de la coleccion a relacionar
                pipeline: [
                  {
                    $match: {
                      estado: "No Leido",
                    },
                  },
                ],
                as: "data_notificaciones", //Nombre del campo donde se insertara todos los documentos relacionados
              },
            },
            {
              $unwind: "$data_notificaciones",
            },
            {
              $replaceRoot: {
                newRoot: "$data_notificaciones",
              },
            },
            {
              $group: {
                _id: "$destinatario",
                total: { $sum: 1 },
              },
            },
          ],
          pedidos: [
            // Pedidos del horeca y filtra por estado
            {
              $lookup: {
                from: "pedidos", //Nombre de la colecccion a relacionar
                localField: "_id", //Nombre del campo de la coleccion actual
                foreignField: "usuario_horeca", //Nombre del campo de la coleccion a relacionar
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
                      _id: "$_id",
                      estado: "$estado",
                      usuario_horeca: "$usuario_horeca",
                    },
                  },
                ],
                as: "data_pedidos", //Nombre del campo donde se insertara todos los documentos relacionados
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
                _id: "$usuario_horeca",
                total: { $sum: 1 },
              },
            },
          ],
          sugeridos: [
            // Pedidos del horeca y filtra por estado
            {
              $lookup: {
                from: "pedidos", //Nombre de la colecccion a relacionar
                localField: "_id", //Nombre del campo de la coleccion actual
                foreignField: "usuario_horeca", //Nombre del campo de la coleccion a relacionar
                pipeline: [
                  {
                    $match: { estado: "Sugerido" },
                  },
                  {
                    $project: {
                      _id: "$_id",
                      estado: "$estado",
                      usuario_horeca: "$usuario_horeca",
                    },
                  },
                ],
                as: "data_pedidos", //Nombre del campo donde se insertara todos los documentos relacionados
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
                _id: "$usuario_horeca",
                total: { $sum: 1 },
              },
            },
          ],
          trabajadores_pend: [
            // Puntos asociados al trabajador
            {
              $lookup: {
                from: "trabajadors", //Nombre de la colecccion a relacionar
                localField: "_id", //Nombre del campo de la coleccion actual
                foreignField: "usuario_horeca", //Nombre del campo de la coleccion a relacionar
                pipeline: [
                  {
                    $match: {
                      solicitud_vinculacion: "Pendiente",
                    },
                  },
                ],
                as: "data_trabajador", //Nombre del campo donde se insertara todos los documentos relacionados
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
                _id: "$solicitud_vinculacion",
                total: { $sum: 1 },
              },
            },
          ],
          solicitudes_dist: [
            {
              $lookup: {
                from: "punto_entregas", //Nombre de la colecccion a relacionar
                localField: "_id", //Nombre del campo de la coleccion actual
                foreignField: "usuario_horeca", //Nombre del campo de la coleccion a relacionar
                pipeline: [
                  {
                    $project: {
                      _id: "$_id",
                    },
                  },
                ],
                as: "data_puntos", //Nombre del campo donde se insertara todos los documentos relacionados
              },
            },
            {
              $unwind: "$data_puntos",
            },
            {
              $replaceRoot: {
                newRoot: "$data_puntos",
              },
            },
            {
              $lookup: {
                from: "distribuidores_vinculados", //Nombre de la colecccion a relacionar
                localField: "_id", //Nombre del campo de la coleccion actual
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
              $unwind: "$data_vinculacion",
            },
            {
              $replaceRoot: {
                newRoot: "$data_vinculacion",
              },
            },
            {
              $group: {
                _id: "$estado",
                total: { $sum: 1 },
              },
            },
          ],
        },
      },
    ]).exec(callback);
  },
};

const Usuario_horeca = (module.exports = mongoose.model(
  "Usuario_horeca",
  Usuario_horecaSchema
));

module.exports.getDistinctUsuariobyField = function (field, callback) {
  Usuario_horeca.find().distinct(field).exec(callback);
};

module.exports.getUsuariosByCorreo = function (correo, callback) {
  const query = { correo: correo };
  Usuario_horeca.findOne(query, callback);
};

module.exports.compararClave = function (usuarioClave, hash, callback) {
  bcrypt.compare(usuarioClave, hash, (err, isMatch) => {
    if (err) throw err;
    callback(null, isMatch);
  });
};

module.exports.agregarUsuario = function (nuevoUsuario, callback) {
  bcrypt.genSalt(10, (err, salt) => {
    bcrypt.hash(nuevoUsuario.clave, salt, (err, hash) => {
      if (err) throw err;
      nuevoUsuario.clave = hash;
      /** Valida que el correo no se repita en un horeca */
      var Usuario_horecaSchema = mongoose.model("Usuario_horeca");
      Usuario_horecaSchema.find({ correo: nuevoUsuario.correo }, (err, res) => {
        if (res.length == 0) {
          /** Valida que el correo no se repita en un distribuidor */
          var DistribuidorSchema = mongoose.model("Distribuidor");
          DistribuidorSchema.find(
            { correo: nuevoUsuario.correo },
            (err, res) => {
              if (res.length == 0) {
                /** Valida que el correo no se repita en un trabajador */
                var TrabajadorSchema = mongoose.model("Trabajador");
                TrabajadorSchema.find(
                  { correo: nuevoUsuario.correo },
                  (err, res) => {
                    if (res.length == 0) {
                      const Usuario_horeca = new this(nuevoUsuario);
                      Usuario_horeca.save(callback);
                    } else {
                      callback(
                        "El correo electronico ya existe para un un trabajador registrado",
                        null
                      );
                    }
                  }
                );
              } else {
                callback(
                  "El correo electronico ya existe para un distribuidor registrado",
                  null
                );
              }
            }
          );
        } else {
          callback(
            "El usuario ya existe para un establecimiento registrado",
            null
          );
        }
      });
    });
  });
};

module.exports.getUserById = function (id, callback) {
  const query = { _id: id };
  Usuario_horeca.findOne(query, callback);
};
