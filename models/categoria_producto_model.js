const mongoose = require("mongoose");
const ObjectId = require("mongoose").Types.ObjectId;
// Schema

const Categoria_productoSchema = mongoose.Schema({
    nombre: {type: String, required: true},
    lineas_producto: {
        type: [mongoose.Schema.Types.ObjectId],
        ref: "Linea_producto",
        required: false,
    },
    createdAt: {type: Date, required: false, default: Date.now},
    logoOn: {type: String, required: false},
    logoOff: {type: String, required: false},
    estado: {type: String, required: false, default: 'Activo'},
});

Categoria_productoSchema.statics = {
  
};

