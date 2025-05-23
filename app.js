
  require("dotenv").config();
  const express = require("express");
  const bodyParser = require("body-parser");
  const cors = require("cors");
  const mongoose = require("mongoose");
  const passport = require("passport");
  const moment = require("moment");
  const momentTZ = require('moment-timezone');
  const app = express();
  const port = process.env.PORT || 8085;
  app.use(express.json());

  app.use(
    bodyParser.json({
      limit: "10mb",
      extended: true,
    })
  );
  
  app.use(
    bodyParser.urlencoded({
      limit: "10mb",
      extended: true,
    })
  );
  
  const whitelist = [
    "http://localhost:4200",
    "http://localhost:4201",

  ];
  const options = {
    origin: (origin, callback) => {
      if (whitelist.includes(origin) || !origin) {
        callback(null, true);
      } else {
        callback(new Error(`Origin ${origin} not allowed due to CORS policy`));
      }
    },
  };
  app.use(cors(options));
  
  /** Procesos Socket Chat */
  const http = require("http").createServer(app);
  const io = require("socket.io")(http, {
    cors: {
      origin: true,
      credentials: true,
      methods: ["GET", "POST"],
    },
  });
  http.listen(3000, () => {
    console.log("listening on *:3000");
  });
  const config = require("./config/database");
  mongoose.connect(config.database);
  mongoose.connection.on("connected", () => {
    console.log("Connected to database");
  });
  mongoose.connection.on("error", (err) => {
    console.log("Database error: " + err);
  });
  app.use(passport.initialize());
  app.use(passport.session());
  const router = express.Router();
  app.use("/api", router);
  require("./routes/api/pedido")(router);

