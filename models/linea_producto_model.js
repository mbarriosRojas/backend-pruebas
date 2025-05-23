const mongoose = require('mongoose');
const ObjectId = require("mongoose").Types.ObjectId;
// Schema

const Linea_productoSchema = mongoose.Schema({
    nombre: {type: String, required: true},
    categoria: {
        type: mongoose.Schema.Types.ObjectId,
        ref: "Categoria_producto",
        required: true,
    },
    estado: {type: String, required: false, default: 'Activo'},
    createdAt: {type: Date, required: false, default: Date.now}
});

Linea_productoSchema.statics = {
    get: function (query, callback) {
        this.findOne(query).exec(callback);
    },
    getAll: function (query, callback) {
        this.find(query)
        .populate("categoria")
        .exec(callback);
    },
    getAd: function (query, callback) {
        const d = new Date();
        const fecha_referencia = d.setMonth(d.getMonth() - 3);
        this.aggregate([
            {
                $match: {_id: new ObjectId(query),},
            },
            {
                $lookup: {
                    from: 'categoria_productos', //Nombre de la colecccion a relacionar
                    localField: "_id", //Nombre del campo de la coleccion actual
                    foreignField: "lineas_producto", //Nombre del campo de la coleccion a relacionar
                    as: "dataCategoria", //Nombre del campo donde se insertara todos los documentos relacionados
                },
            },
            {
                $sort: {cantidad: -1},
            }
        ]).exec(callback);
    },
    getAllAdmin: function (query, callback) {
        const d = new Date();
        const fecha_referencia = d.setMonth(d.getMonth() - 3);
        this.aggregate([
            {
                $match: {},
            },
            {
                $lookup: {
                    from: 'categoria_productos', //Nombre de la colecccion a relacionar
                    localField: "_id", //Nombre del campo de la coleccion actual
                    foreignField: "lineas_producto", //Nombre del campo de la coleccion a relacionar
                    as: "dataCategoria", //Nombre del campo donde se insertara todos los documentos relacionados
                },
            },
            {
                $lookup: {
                    from: 'productos', //Nombre de la colecccion a relacionar
                    localField: "_id", //Nombre del campo de la coleccion actual
                    foreignField: "linea_producto", //Nombre del campo de la coleccion a relacionar

                    as: "dataProductos", //Nombre del campo donde se insertara todos los documentos relacionados
                },
            },
            {
                $lookup: {
                    from: 'reportepedidos', //Nombre de la colecccion a relacionar
                    localField: "_id", //Nombre del campo de la coleccion actual
                    foreignField: "lineaProducto", //Nombre del campo de la coleccion a relacionar
                    pipeline: [
                        {
                            $match: {},
                        },
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
                    ],
                    as: "domicilios", //Nombre del campo donde se insertara todos los documentos relacionados
                },
            },
            {
                $lookup: {
                    from: 'reportepedidos', //Nombre de la colecccion a relacionar
                    localField: "_id", //Nombre del campo de la coleccion actual
                    foreignField: "lineaProducto", //Nombre del campo de la coleccion a relacionar
                    pipeline: [
                        {
                            $match: {},
                        },
                        {
                            $group: {
                                _id: "$lineaProducto",
                                numEstablecimientos: {$sum: 1},
                            },
                        },
                    ],
                    as: "establecimientosAlcanzados", //Nombre del campo donde se insertara todos los documentos relacionados
                },
            },
            {
                $lookup: {
                    from: 'reportepedidos', //Nombre de la colecccion a relacionar
                    localField: "_id", //Nombre del campo de la coleccion actual
                    foreignField: "lineaProducto", //Nombre del campo de la coleccion a relacionar
                    pipeline: [
                        {
                            $match: {},
                        },
                        {
                            $group: {
                                _id: "$lineaProducto",
                                numSillas: {$sum: "$puntoSillas"},
                            },
                        },
                    ],
                    as: "sillasAlcanzadas", //Nombre del campo donde se insertara todos los documentos relacionados
                },
            },
            {
                $lookup: {
                    from: 'reportepedidos', //Nombre de la colecccion a relacionar
                    localField: "_id", //Nombre del campo de la coleccion actual
                    foreignField: "lineaProducto", //Nombre del campo de la coleccion a relacionar
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
                                costoTotalProductoFinal: {
                                    $multiply: ["$unidadesCompradas", "$costoProductos"],
                                },
                            },
                        },
                        {
                            $project: {
                                categoriaProducto: "$lineaProducto",
                                costoTotalProductoFinal: "$costoTotalProductoFinal",
                            },
                        },
                        {
                            $group: {
                                _id: "$lineaProducto",
                                costoTotalProductoFinal: {$sum: "$costoTotalProductoFinal"},
                            },
                        },
                    ],
                    as: "ventasTotales", //Nombre del campo donde se insertara todos los documentos relacionados
                },
            },
            {
                $sort: {cantidad: -1},
            }
        ]).exec(callback);
    },
    updateById: function (id, updateData, callback) {
        this.findOneAndUpdate(
            {_id: id},
            {$set: updateData},
            {new: true},
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
        const Linea_producto = new this(data);
        Linea_producto.save(callback);
    },
};

const Linea_producto = (module.exports = mongoose.model('Linea_producto', Linea_productoSchema));

module.exports.findLineaByName = function (name, callback) {
    const query = {nombre: name};
    Linea_producto.findOne(query, callback);
}
