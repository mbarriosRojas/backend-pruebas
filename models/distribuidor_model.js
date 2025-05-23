const mongoose = require("mongoose");
// Schema
const TrabajadoresBD_model = require("./trabajador_model");
const DocumentosBD_model = require("./documentos_solicitante_model");
const DistribuidoresVinculadosBD = require("./distribuidores_vinculados_model");
const productosDistribuidor = require("./productos_por_distribuidor_model");
const productos = require("./producto_model");
const linea = require("./linea_producto_model");
const categoria = require("./categoria_producto_model");
const marca = require("./marca_producto_model");
const organizacion = require("./organizacion_model");
const bcrypt = require("bcryptjs");

const DistribuidorSchema = mongoose.Schema({
  nombre: { type: String, required: true },
  correo: { type: String, required: true },
  nit_cc: { type: String, require: true },
  logo: { type: String, required: false },
  descripcion: { type: String, required: true },
  listGrucam: { type: Boolean, default: false },
  tiempo_entrega: { type: String, required: false },
  departamento: { type: String, required: true },
  pais: { type: String, required: false, default: 'Colombia' },
  ciudad: { type: String, required: true },
  direccion: { type: String, required: true },
  urlPago: { type: String, required: false },
  ranking: { type: Number, required: true },
  // Almacena la identificación de los clientes pre-aprobados de un distribuidor. Puede ser tanto NIT como CC.
  clientes_preaprobados: { type: [String], default: [] },
  top_productos: {
    type: [mongoose.Schema.Types.ObjectId],
    ref: "Producto",
    default: [],
  },
  tipo: { type: String, required: false },
  valor_minimo_pedido: { type: Number, required: false },
  cobertura_coordenadas: [
    {
      longitud: { type: Number, required: false, default: 0 },
      latitud: { type: Number, required: false, default: 0 },
    },
  ],
  cant_votos_raking: { type: Number, required: false, default: 0 },
  ranking_gen: [
    {
      name: { type: String, required: false, default: 'Abastecimiento' },
      puntos: { type: Number, required: false, default: 0 },
    },
    {
      name: { type: String, required: false, default: 'Puntualidad' },
      puntos: { type: Number, required: false, default: 0 },
    },
    {
      name: { type: String, required: false, default: 'Precio' },
      puntos: { type: Number, required: false, default: 0 },
    },
  ],
  horario_atencion: { type: String, required: false },
  metodo_pago: { type: String, required: false },
  max_establecimientos: { type: Number, required: false, default: 10 },
  razon_social: { type: String, required: false },
  celular: { type: String, required: true },
  telefono: { type: String, required: false },
  solicitud_vinculacion: {
    type: String,
    required: false,
    default: "Pendiente",
  },
  tipo_persona: {
    type: String,
    required: true,
    default: "Natural",
    enum: ["Natural", "Juridica"],
  },
  zonas_cobertura: {
    type: {
      type: String,
      default: 'MultiPolygon'
    },
    coordinates: [[[[Number]]]],
    required: false
  },
  datos_poligono: [
    {
      tipo_promesa: { type: String, required: false },
      valor_promesa: { type: String, required: false },
      valor_pedido: { type: Number, required: false },
    }
  ],
  createdAt: { type: Date, required: false, default: Date.now },
});

DistribuidorSchema.statics = {
  get: function (query, callback) {
    this.findOne(query).exec(callback);
  },
  getDistNit: function (query, callback) {
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
    const Distribuidor = new this(data);
    Distribuidor.save(callback);
  },
  getDistribuidorById: function (obj, callback) {
    var ObjectId = require("mongoose").Types.ObjectId;
    this.findOne({ _id: new ObjectId(obj.distribuidor) }).exec(callback);
  },
  getPuntosMap: function (query, callback) {
    this.aggregate([
      {
        $match: {},
      },
    ]).exec(callback);
  },
  ciudadesDistribuidor: function (query, callback) {
    this.aggregate([
      {
        $match: {},
      },
      {
        $group: {
          _id: "$ciudad",
        },
      },
    ]).exec(callback);
  },
  // Detalle de un distribuidor visto desde una organización
  getDetalleDistribuidorParaOrganizacion: function (query, callback) {
    var ObjectId = require("mongoose").Types.ObjectId;
    this.aggregate([
      {
        $match: {
          _id: new ObjectId(query._id),
        },
      },
      {
        $lookup: {
          from: "pedidos", //Nombre de la colecccion a relacionar
          localField: "_id", //Nombre del campo de la coleccion actual
          foreignField: "distribuidor", //Nombre del campo de la coleccion a relacionar
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
              $group: {
                _id: "$distribuidor",
                total: { $avg: "$total_pedido" },
              },
            },
          ],
          as: "data_distribuidor", //Nombre del campo donde se insertara todos los documentos relacionados
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
        $lookup: {
          from: "trabajadors",
          localField: "_id",
          foreignField: "distribuidor",
          as: "data_trabajadores",
        },
      },
      {
        $facet: {
          distribuidor: [
            {
              $project: {
                nombre: "$nombre",
                logo: "$logo",
                correo: "$correo",
                tiempo_entrega: "$tiempo_entrega",
                ciudad: "$ciudad",
                tipo: "$tipo",
                valor_minimo_pedido: "$valor_minimo_pedido",
                celular: "$celular",
                direccion: "$direccion",
                horario_atencion: "$horario_atencion",
                metodo_pago: "$metodo_pago",
                telefono: "$telefono",
              },
            },
          ],
          calificacion: [
            {
              $project: {
                calificacion: {
                  $arrayElemAt: ["$data_distribuidor.total", 0],
                },
              },
            },
          ],
          trabajadores: [
            {
              $project: {
                total: {
                  $size: ["$data_trabajadores"],
                },
              },
            },
            /*{
              $unwind: "$data_vinculacion",
            },
            {
              $replaceRoot: {
                newRoot: "$data_vinculacion",
              },
            },
            {
              $unwind: "$vendedor",
            },
            {
              $group: {
                _id: "$vendedor",
              },
            },
            {
              $addFields: {
                group_flag: "flag",
              },
            },
            {
              $group: {
                _id: "$group_flag",
                total: { $sum: 1 },
              },
            },*/
          ],
          promedio_ventas: [
            {
              $project: {
                promedio_ventas: {
                  $arrayElemAt: ["$data_distribuidor.total", 0],
                },
              },
            },
          ],
          total_productos: [
            {
              $lookup: {
                from: "productos",
                localField: "_id",
                foreignField: "codigo_distribuidor",
                pipeline: [
                  {
                    $match: {
                      estadoActualizacion: "Aceptado",
                      promocion: false,
                      saldos: false,
                    },
                  },
                ],
                as: "data_productos",
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
              $group: {
                _id: "$codigo_distribuidor",
                total: { $sum: 1 },
              },
            },
          ],
          establecimientos: [
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
                _id: "$punto_entrega",
              },
            },
            {
              $lookup: {
                from: "punto_entregas",
                localField: "_id",
                foreignField: "_id",
                as: "data_punto",
              },
            },
            {
              $addFields: {
                "data_punto.group_flag": "flag",
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
                _id: "$group_flag",
                total: { $sum: 1 },
                sillas: { $sum: "$sillas" },
              },
            },
          ],
        },
      },
      {
        $project: {
          distribuidor: {
            $arrayElemAt: ["$distribuidor", 0],
          },
          calificacion: {
            $arrayElemAt: ["$calificacion.calificacion", 0],
          },
          trabajadores: {
            $arrayElemAt: ["$trabajadores.total", 0],
          },
          promedio_ventas: {
            $arrayElemAt: ["$promedio_ventas.promedio_ventas", 0],
          },
          total_productos: {
            $arrayElemAt: ["$total_productos.total", 0],
          },
          sillas: {
            $arrayElemAt: ["$establecimientos.sillas", 0],
          },
          total_establecimientos: {
            $arrayElemAt: ["$establecimientos.total", 0],
          },
        },
      },
    ]).exec(callback);
  },
  precioCiudades: function (query, callback) {
    this.aggregate([
      {
        $match: { ciudad: query.ciudad },
      },
      {
        $lookup: {
          from: productosDistribuidor.collection.name, //Nombre de la colecccion a relacionar
          localField: "_id", //Nombre del campo de la coleccion actual
          foreignField: "distribuidor", //Nombre del campo de la coleccion a relacionar
          pipeline: [
            {
              $lookup: {
                from: productos.collection.name, //Nombre de la colecccion a relacionar
                localField: "productos", //Nombre del campo de la coleccion actual
                foreignField: "_id", //Nombre del campo de la coleccion a relacionar
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
          "productosPorDistribuidor.nombreDistribuidor": "$nombre",
        },
      },
      {
        $addFields: {
          "productosPorDistribuidor.nitDistribuidor": "$nit_cc",
        },
      },
      {
        $unwind: "$productosPorDistribuidor",
      },
      {
        $replaceRoot: {
          newRoot: "$productosPorDistribuidor",
        },
      },
      {
        $sort: { createdAt: -1 },
      },
      {
        $lookup: {
          from: marca.collection.name, //Nombre de la colecccion a relacionar
          localField: "marca_producto", //Nombre del campo de la coleccion actual
          foreignField: "_id", //Nombre del campo de la coleccion a relacionar
          as: "dataMarca", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
      {
        $lookup: {
          from: categoria.collection.name, //Nombre de la colecccion a relacionar
          localField: "categoria_producto", //Nombre del campo de la coleccion actual
          foreignField: "_id", //Nombre del campo de la coleccion a relacionar
          as: "dataCat", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
      {
        $lookup: {
          from: linea.collection.name, //Nombre de la colecccion a relacionar
          localField: "linea_producto", //Nombre del campo de la coleccion actual
          foreignField: "_id", //Nombre del campo de la coleccion a relacionar
          as: "dataLinea", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
      {
        $lookup: {
          from: organizacion.collection.name, //Nombre de la colecccion a relacionar
          localField: "codigo_organizacion", //Nombre del campo de la coleccion actual
          foreignField: "_id", //Nombre del campo de la coleccion a relacionar
          as: "dataOrganizacion", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
      {
        $project: {
          idProduco: "$_id",
          codigo_distribuidor: "$codigo_distribuidor_producto",
          codigo_ft: "$codigo_ft",
          codigo_organizacion: "$codigo_organizacion_producto",
          nombreDistribuidor: "$nombreDistribuidor",
          nitDistribuidor: "$nitDistribuidor",
          nombreProducto: "$nombre",
          lineaProducto: "$dataLinea.nombre",
          marcaProducto: "$dataMarca.nombre",
          categoriaProducto: "$dataCat.nombre",
          organizacion: "$dataOrganizacion.nombre",
          descripcionProducto: "$descripcion",
          tamanio: "$precios.cantidad_medida",
          unidad_medida: "$precios.unidad_medida",
          precio: "$precios.precio_unidad",
          stock: "$precios.inventario_unidad",
          puntosUnidad: "$precios.puntos_ft_unidad",
          inicioPuntos: {
            $dateToString: {
              format: "%Y-%m-%d",
              date: "$fecha_apertura_puntosft",
            },
          },
          cierrePuntos: {
            $dateToString: {
              format: "%Y-%m-%d",
              date: "$fecha_cierre_puntosft",
            },
          },
        },
      },
    ]).exec(callback);
  },
  // Agrupar por busqueda
  get_agrupados_dist: function (query, callback) {
    if (query == "all") {
      this.find({}).exec(callback);
    } else {
      let data_arr = query.split(",");
      let busqueda = [];
      for (let tipo of data_arr) {
        if (tipo == "ASEO") {
          busqueda.push({ tipo: "Aseo y otros" });
        }
        if (tipo == "BEBIDAS") {
          busqueda.push({ tipo: "Bebidas" });
        }
        if (tipo == "CARNICOS") {
          busqueda.push({ tipo: "Cárnicos" });
        }
        if (tipo == "ESPECIALIZADO") {
          busqueda.push({ tipo: "Especializado - general" });
        }
        if (tipo == "FRUTAS") {
          busqueda.push({ tipo: "Frutas y verduras" });
        }
        if (tipo == "LACTEOS") {
          busqueda.push({ tipo: "Lácteos" });
        }
        if (tipo == "LICORES") {
          busqueda.push({
            tipo: "Licores",
          });
        }
        if (tipo == "MAQUINARIA") {
          busqueda.push({ tipo: "Maquinaria e implementos" });
        }
      }
      this.find({
        $or: busqueda,
      }).exec(callback);
    }
  },
  distribuidoresNew: function (query, callback) {
    this.aggregate([
      {
        $match: {},
      },
      {
        $sort: {
          createdAt: -1,
        },
      },
      {
        $lookup: {
          from: "trabajadors", //Nombre de la colecccion a relacionar
          localField: "_id", //Nombre del campo de la coleccion actual
          foreignField: "distribuidor", //Nombre del campo de la coleccion a relacionar
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
          tipo_negocio: "$tipo",
          tipo_usuario: "$representante.tipo_trabajador",
          tipo_documento: "$nit_cc",
          razon_social: "$razon_social",
          descripcion: "$descripcion",
          nombres01: "$nombre",
          ciudad: "$ciudad",
          direccion: "$direccion",
          apellidos01: "$apellido",
          celular: "$celular",
          correo: "$representante.correo",
          nombres02: "$representante.nombres",
          apellidos02: "$representante.apellidos",
          dataRepresentante: "$representante",
          dataRepresentanteApe: "$representante",
          usuarioClave: "$correo",
          estado: "$solicitud_vinculacion",
        },
      },
    ]).exec(callback);
  },
  distribuidorLogoNombre: function (query, callback) {
    var ObjectId = require("mongoose").Types.ObjectId;
    this.aggregate([
      {
        $project: {
          nombre: "$nombre",
          logo: "$logo",
        },
      },
    ]).exec(callback);
  },
  getDataDistribuidor: function (query, callback) {
    var ObjectId = require("mongoose").Types.ObjectId;
    this.aggregate([
      {
        $match: {
          _id: new ObjectId(query),
        },
      },
      {
        $lookup: {
          from: TrabajadoresBD_model.collection.name, //Nombre de la colecccion a relacionar
          localField: "_id", //Nombre del campo de la coleccion actual
          foreignField: "distribuidor", //Nombre del campo de la coleccion a relacionar
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
          from: DocumentosBD_model.collection.name, //Nombre de la colecccion a relacionar
          localField: "_id", //Nombre del campo de la coleccion actual
          foreignField: "usuario_distribuidor", //Nombre del campo de la coleccion a relacionar
          as: "documentacion", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
      {
        $project: {
          dataUser: {
            nombre: "$nombre",
            correo: "$correo",
            nit_cc: "$nit_cc",
            logo: "$logo",
            descripcion: "$descripcion",
            tiempo_entrega: "$tiempo_entrega",
            departamento: "$departamento",
            ciudad: "$ciudad",
            direccion: "$direccion",
            listGrucam: "$listGrucam",
            ranking: "$ranking",
            tipo: "$tipo",
            tipo_persona: "$tipo_persona",
            valor_minimo_pedido: "$valor_minimo_pedido",
            cobertura_coordenadas: "$cobertura_coordenadas",
            horario_atencion: "$horario_atencion",
            metodo_pago: "$metodo_pago",
            razon_social: "$razon_social",
            celular: "$celular",
            solicitud_vinculacion: "$solicitud_vinculacion",
            telefono: "$telefono",
          },
          representantes: "$representante",
          documentos: "$documentacion",
        },
      },
    ]).exec(callback);
  },
  graficaDistribuidoresXTipo: function (query, callback) {
    this.aggregate([
      {
        $match: {
          ciudad: query.ciudad,
          createdAt: {
            $gte: new Date(query.inicio),
            $lte: new Date(query.fin),
          },
        },
      },
      {
        $group: {
          _id: "$tipo",
          cantidad: { $sum: 1 },
        },
      },
      {
        $sort: { cantidad: -1 },
      },
    ]).exec(callback);
  },
  /** Productos por ciudad seleccionada */
  getProductosChequeoPrecios: function (req, callback) {
    const query_ciudad = { ciudad: req.ciudad };
    this.aggregate([
      {
        $match: query_ciudad,
      },
      {
        $lookup: {
          from: productosDistribuidor.collection.name, //Nombre de la colecccion a relacionar
          localField: "_id", //Nombre del campo de la coleccion actual
          foreignField: "distribuidor", //Nombre del campo de la coleccion a relacionar
          as: "data_productos_distribuidor", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
      {
        $lookup: {
          from: productos.collection.name, //Nombre de la colecccion a relacionar
          localField: "data_productos_distribuidor.productos", //Nombre del campo de la coleccion actual
          foreignField: "_id", //Nombre del campo de la coleccion a relacionar
          pipeline: [
            {
              $match: {
                estadoActualizacion: "Aceptado",
                promocion: false,
                saldos: false,
              },
            },
            {
              $project: {
                nombre: "$nombre",
                estadoActualizacion: "$estadoActualizacion",
                codigo_ft: "$codigo_ft",
                descripcion: "$descripcion",
                precios: "$precios",
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
    ]).exec(callback);
  },
  /** Productos por ciudad seleccionada y cod feat */
  getProductosChequeoPreciosCodFeat: function (req, callback) {
    const query_ciudad = { ciudad: req.ciudad };
    this.aggregate([
      {
        $match: query_ciudad,
      },
      {
        $lookup: {
          from: productosDistribuidor.collection.name, //Nombre de la colecccion a relacionar
          localField: "_id", //Nombre del campo de la coleccion actual
          foreignField: "distribuidor", //Nombre del campo de la coleccion a relacionar
          as: "data_productos_distribuidor", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
      {
        $lookup: {
          from: productos.collection.name, //Nombre de la colecccion a relacionar
          localField: "data_productos_distribuidor.productos", //Nombre del campo de la coleccion actual
          foreignField: "_id", //Nombre del campo de la coleccion a relacionar
          pipeline: [
            {
              $match: {
                estadoActualizacion: "Aceptado",
                codigo_ft: req.codigo_ft,
                promocion: false,
                saldos: false,
              },
            },
            {
              $project: {
                nombre: "$nombre",
                estadoActualizacion: "$estadoActualizacion",
                codigo_ft: "$codigo_ft",
                fotos: "$fotos",
                descripcion: "$descripcion",
                codigo_distribuidor: "$codigo_distribuidor",
                precio_unidad: "$precios.precio_unidad",
                unidad_medida: "$precios.unidad_medida",
                cantidad_medida: "$precios.cantidad_medida",
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
    ]).exec(callback);
  },

  getProductosPorCiudad: function (req, callback) {
    var ObjectId = require("mongoose").Types.ObjectId;
    this.aggregate([
      {
        $match: {
          ciudad: req,
        },
      },
      {
        $lookup: {
          from: productosDistribuidor.collection.name, //Nombre de la colecccion a relacionar
          localField: "_id", //Nombre del campo de la coleccion actual
          foreignField: "distribuidor", //Nombre del campo de la coleccion a relacionar
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
        $lookup: {
          from: productos.collection.name, //Nombre de la colecccion a relacionar
          localField: "productos", //Nombre del campo de la coleccion actual
          foreignField: "_id", //Nombre del campo de la coleccion a relacionar
          as: "detalle_producto", //Nombre del campo donde se insertara todos los documentos relacionados
        },
      },
      {
        $unwind: "$detalle_producto",
      },
      {
        $replaceRoot: {
          newRoot: "$detalle_producto",
        },
      },
    ]).exec(callback);
  },
  agregarUsuario: function (nuevoUsuario, callback) {
    /** Valida que el correo no se repita en un horeca */
    var Usuario_horecaSchema = mongoose.model("Usuario_horeca");
    Usuario_horecaSchema.find({ correo: nuevoUsuario.correo }, (err, res) => {
      if (res.length == 0) {
        /** Valida que el correo no se repita en un trabajador */
        var TrabajadorSchema = mongoose.model("Trabajador");
        TrabajadorSchema.find({ correo: nuevoUsuario.correo }, (err, res) => {
          if (
            res.length == 0 ||
            res.tipo_trabajador === "PROPIETARIO/REP LEGAL"
          ) {
            const Usuario_horeca = new this(nuevoUsuario);
            Usuario_horeca.save(callback);
          } else {
            callback(
              "El correo electronico ya existe para un trabajador registrado",
              null
            );
          }
        });
      } else {
        callback(
          "El correo electronico ya existe para un establecimiento registrado",
          null
        );
      }
    });
  },
  /******************************** Grafica Horeca  ********************************/
  /** Tabla con data de los establecimientos por tipo de ngocio */
  getInformeBarraDistribuidoresNegocio: function (query, callback) {
    var ObjectId = require("mongoose").Types.ObjectId;
    this.aggregate([
      {
        $group: {
          _id: "$tipo",
          total: { $sum: 1 },
        },
      },
      {
        $sort: { total: -1 },
      },
    ]).exec(callback);
  },
  countDistribuidores: function (req, callback) {
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
  getDistribuidoresGeneralesFiltro: function (query, callback) {
    this.aggregate([
      {
        $match: query
      },
      {
        $project: {
          nombre: "$nombre"
        }
      },
      {
        $lookup: {
          from: 'productos', //Nombre de la colecccion a relacionar
          localField: "_id", //Nombre del campo de la coleccion actual
          foreignField: "codigo_distribuidor", //Nombre del campo de la coleccion a relacionar
          as: "categoriasDistribuidor", //Nombre del campo donde se insertara todos los documentos relacionados
          pipeline: [
            {
              $match: {
                estadoActualizacion: 'Aceptado'
              }
            },
            {
              $group: {
                _id: "$categoria_producto",
              },
            },
            {
              $lookup: {
                from: 'categoria_productos', //Nombre de la colecccion a relacionar
                localField: "_id", //Nombre del campo de la coleccion actual
                foreignField: "_id", //Nombre del campo de la coleccion a relacionar
                as: "dataCat", //Nombre del campo donde se insertara todos los documentos relacionados
                pipeline: [
                  {
                    $project: {
                      nombre: "$nombre"
                    },
                  }
                ]
              },
            },
            {
              $unwind: "$dataCat",
            },
            {
              $replaceRoot: {
                newRoot: "$dataCat",
              },
            },
          ],
        },
      },
      {
        $project: {
          data_distribuidor: [{ _id: "$_id", nombre: "$nombre" }],
          categoriasDistribuidor: "$categoriasDistribuidor"
        }
      }
    ]).exec(callback);
  },
  valorMinimo: function (query, callback) {
    var ObjectId = require("mongoose").Types.ObjectId;
    this.aggregate([
      {
        $match: {
          _id: new ObjectId(query),
        },
      },
      {
        $project: {
          valor_minimo: "$valor_minimo_pedido",
        },
      },
    ]).exec(callback);
  }
};

const Distribuidor = (module.exports = mongoose.model(
  "Distribuidor",
  DistribuidorSchema
));
