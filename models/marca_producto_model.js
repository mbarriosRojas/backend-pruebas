const mongoose = require("mongoose");
// Schema

const Marca_productoSchema = mongoose.Schema({
    nombre: { type: String, required: true },
    organizacion: {
        type: mongoose.Schema.Types.ObjectId,
        ref: "Organizacion",
        required: false,
    },
    categoria: {
        type: mongoose.Schema.Types.ObjectId,
        ref: "Categoria_producto",
        required: false,
    },
    linea: {
        type: mongoose.Schema.Types.ObjectId,
        ref: "Linea_producto",
        required: false,
    },
    estado: {
        type: String,
        required: false,
        default: "Aprobado",
        enum: ["Aprobado", "Inactiva"],
    },
    createdAt: { type: Date, required: false, default: Date.now },
});

Marca_productoSchema.statics = {
    get: function (query, callback) {
        this.findOne(query)
            .populate("organizacion")
            .populate("categoria")
            .populate("linea")
            .exec(callback);
    },
    getAll: function (query, callback) {
        this.find(query)
            .populate("organizacion")
            .populate("categoria")
            .populate("linea")
            .exec(callback);
    },
    getAllComplete: function (query, callback) {
        const fin = new Date();
        const f = new Date();
        const fechaReferencia = f.setMonth(f.getMonth() - 3);
        const inicio = new Date(fechaReferencia);
        this.aggregate([
            {
                $match: {},
            },
            {
                $lookup: {
                    from: "reportepedidos", //Nombre de la colecccion a relacionar
                    localField: "_id", //Nombre del campo de la coleccion actual
                    foreignField: "marcaProducto", //Nombre del campo de la coleccion a relacionar
                    pipeline: [
                        {
                            $match: {
                                createdAt: {
                                    $gte: new Date(inicio),
                                    $lte: new Date(fin),
                                },

                            },
                        },
                    ],
                    as: "dataPedido", //Nombre del campo donde se insertara todos los documentos relacionados
                },
            },
            {
                $lookup: {
                    from: "reportepedidos", //Nombre de la colecccion a relacionar
                    localField: "_id", //Nombre del campo de la coleccion actual
                    foreignField: "marcaProducto", //Nombre del campo de la coleccion a relacionar
                    pipeline: [
                        {
                            $facet: {
                                con_domicilio: [
                                    {
                                        $match: {
                                            puntoDomicilio: true,
                                        },
                                    },
                                ],
                                sin_Domicilio: [
                                    {
                                        $match: {
                                            puntoDomicilio: false,
                                        },
                                    },
                                ],
                            },
                        },
                        {
                            $project: {
                                '01': { $size: "$sin_Domicilio" },
                                '02': { $size: "$con_domicilio" },

                            },
                        },
                    ],
                    as: "CalculoDomicilios", //Nombre del campo donde se insertara todos los documentos relacionados
                },
            },
            {
                $lookup: {
                    from: "productos", //Nombre de la colecccion a relacionar
                    localField: "_id", //Nombre del campo de la coleccion actual
                    foreignField: "marca_producto", //Nombre del campo de la coleccion a relacionar
                    as: "DataProducto", //Nombre del campo donde se insertara todos los documentos relacionados
                },
            },
            {
                $lookup: {
                    from: "organizacions", //Nombre de la colecccion a relacionar
                    localField: "organizacion", //Nombre del campo de la coleccion actual
                    foreignField: "_id", //Nombre del campo de la coleccion a relacionar
                    as: "dataOrganizacion", //Nombre del campo donde se insertara todos los documentos relacionados
                },
            },
            {
                $lookup: {
                    from: "categoria_productos", //Nombre de la colecccion a relacionar
                    localField: "categoria", //Nombre del campo de la coleccion actual
                    foreignField: "_id", //Nombre del campo de la coleccion a relacionar
                    as: "dataCategoria", //Nombre del campo donde se insertara todos los documentos relacionados
                },
            },
            {
                $lookup: {
                    from: "linea_productos", //Nombre de la colecccion a relacionar
                    localField: "linea", //Nombre del campo de la coleccion actual
                    foreignField: "_id", //Nombre del campo de la coleccion a relacionar
                    as: "dataLinea", //Nombre del campo donde se insertara todos los documentos relacionados
                },
            },

            {
                $project: {
                    cantidadProductos: { $size: "$DataProducto" },
                    linea: "$dataLinea.nombre",
                    categoria: "$dataCategoria.nombre",
                    organizacion: "$dataOrganizacion.nombre",
                    marca: "$nombre",
                    sillas: { $sum: "$dataPedido.puntoSillas" },
                    compras: { $sum: "$dataPedido.totalCompra" },
                    docimilio: "$CalculoDomicilios.01",
                    SinDocimilio: '$CalculoDomicilios.02',
                },
            },
            /*{
              $project: {
                cantidadProductos: { $size: "$producto" },
                linea: { $size: "$dataLinea.nombre" },
                categoria: { $size: "$dataCategoria.nombre" },
                organizacion: { $size: "$dataOrganizacion.nombre" },
                marca: { $size: "$nombre" },
              },
            },*/
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
    removeById: function (removeData, callback) {
        this.findOneAndRemove(removeData, callback);
    },
    removeAll: function (query, callback) {
        this.remove(query).exec(callback);
    },
    create: function (data, callback) {
        const Marca_producto = new this(data);
        Marca_producto.save(callback);
    },
    countMarcas: function (req, callback) {
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
};

const Marca_producto = (module.exports = mongoose.model(
    "Marca_producto",
    Marca_productoSchema
));

module.exports.findMarcaByName = function (name, callback) {
    const query = { nombre: name };
    Marca_producto.findOne(query, callback);
};
