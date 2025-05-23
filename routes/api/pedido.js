const Pedido = require("../../controllers/pedido_controller");

module.exports = function (router) {
router.get("/test-conexion", (req, res) => {
    res.status(200).send("Conexión exitosa al servidor!");
  });
};
