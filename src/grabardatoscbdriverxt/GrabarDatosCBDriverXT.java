/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package grabardatoscbdriverxt;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.time.LocalDateTime;
import java.util.logging.Level;
import java.util.logging.LogManager;
import java.util.logging.Logger;

/**
 * @author RUBEN.SEGARRA
 */
public class GrabarDatosCBDriverXT {

    private static final String SQLSERVER_IP = "192.168.0.223";
    private static final String SQLSERVER_PORT = "1433";
    private static final String SQLSERVER_DATABASE = "XB01";
    private static final String SQLSERVER_USER = "exos";
    private static final String SQLSERVER_PASSWORD = "Pass3x0s";
    //private static final String SQLSERVER_URL = "jdbc:sqlserver://" + sqlserverIp + ":" + sqlserverPort + ";database=" + sqlserverDatabaseName + ";integratedSecurity=true;";
    private static final String SQLSERVER_URL = "jdbc:sqlserver://" + SQLSERVER_IP + ":" + SQLSERVER_PORT + ";database=" + SQLSERVER_DATABASE
            + ";user=" + SQLSERVER_USER + ";password=" + SQLSERVER_PASSWORD + ";";

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws IOException {

        System.out.println("- Comienzo - " + LocalDateTime.now());

        File folder = new File("./logs");
        if (!folder.exists() || !folder.isDirectory()) {
            System.out.println("creando ruta" + folder);
            folder.mkdirs();
        }
        InputStream is = GrabarDatosCBDriverXT.class.getResourceAsStream("log.properties");
        LogManager.getLogManager().readConfiguration(is);

        String path = System.getProperty("java.library.path");

        // PRODUCCION
        path = "/home/exos/planificador/grabarDatosCBDriverXT/dist/lib/" + ";" + path;
        //path = "C:\\Program Files\\EXOS\\grabarDatosCBDriverXT\\dist\\lib\\" + ";" + path;
        // HYDRA
        // path = "C:\\Users\\Exos\\Desktop\\grabarDatosCBDriverXT\\dist\\lib\\" + ";" + path;
        // LOCAL
        // path = "C:\\Users\\alexgpozo\\Documents\\NetBeansProjects\\GrabarDatosCBDriverXT\\dist\\lib\\" + path;

        System.setProperty("java.library.path", path);

        try {
            final Field sysPathsField = ClassLoader.class.getDeclaredField("sys_paths");
            sysPathsField.setAccessible(true);
            sysPathsField.set(null, null);
        } catch (IllegalAccessException | IllegalArgumentException | NoSuchFieldException | SecurityException ex) {
            throw new RuntimeException(ex);
        }

        if (args.length == 1) {
            String[] parametros = args[0].split(";");
            if (parametros.length == 11) {
                grabarXT(parametros[0], parametros[1], parametros[2], parametros[3], parametros[4], parametros[5], parametros[6],
                        parametros[7], parametros[8], parametros[9], parametros[10]);
                Logger.getLogger(GrabarDatosCBDriverXT.class.getName()).log(Level.INFO, "Valores grabados");
            } else {
                Logger.getLogger(GrabarDatosCBDriverXT.class.getName()).log(Level.SEVERE, "Valores incorrectos");
            }
        } else {
            grabarXT("estado_ruta", "0", "39.4326, -0.3491", "179", "2022-08-06 05:55:00", "", "2022416649", "", "945748", "", "0320HXH");
            Logger.getLogger(GrabarDatosCBDriverXT.class.getName()).log(Level.SEVERE, "Parámetros incorrectos");
        }

        System.out.println("- Fin - " + LocalDateTime.now());

    }

    private static void grabarXT(String estado, String tipo, String posicion, String codigoConductor, String horaInicio, String horaFin,
            String codigoHojaRuta, String litrosRepostaje, String kmsInicio, String kmsFin, String matriculaCabeza) {
        String insertIntoXT = "INSERT INTO [CBDriver_Viajes] ([codigo_conductor], [fecha_hora], [matricula_cabeza], [codigo_HR], [estado], [litros_repostaje], [kilometros], [posicion_gps]) "
                + " VALUES (?, ?, ?, ?, ?, ?, ?, ?); ";

        try (Connection sqlserverConnection = DriverManager.getConnection(SQLSERVER_URL)) {
            try (PreparedStatement ps = sqlserverConnection.prepareStatement(insertIntoXT)) {
                ps.setString(1, codigoConductor);
                ps.setString(3, matriculaCabeza);
                ps.setString(4, codigoHojaRuta);
                switch (estado) {
                    case "estado_ruta": {
                        ps.setString(2, horaInicio);
                        ps.setNull(6, Types.DOUBLE);
                        ps.setString(8, posicion);
                        switch (tipo) {
                            case "0": {
                                ps.setString(5, "Inicio ruta");
                                try {
                                    ps.setInt(7, Integer.parseInt(kmsInicio));
                                } catch (Exception e) {
                                    ps.setNull(7, Types.INTEGER);
                                }
                                break;
                            }
                            case "1": {
                                ps.setString(5, "Carga contenedor");
                                ps.setNull(7, Types.INTEGER);
                                break;
                            }
                            case "2": {
                                ps.setString(5, "Llegada fábrica");
                                ps.setNull(7, Types.INTEGER);
                                break;
                            }
                            case "3": {
                                ps.setString(5, "Salida fábrica");
                                ps.setNull(7, Types.INTEGER);
                                break;
                            }
                            case "4": {
                                ps.setString(5, "Descarga contenedor");
                                ps.setNull(7, Types.INTEGER);
                                break;
                            }
                            case "5": {
                                ps.setString(5, "Fin ruta");
                                try {
                                    ps.setInt(7, Integer.parseInt(kmsFin));
                                } catch (Exception e) {
                                    ps.setNull(7, Types.INTEGER);
                                }
                                break;
                            }
                            default: {
                                break;
                            }
                        }
                    }
                    case "control_horario": {
                        ps.setNull(6, Types.DOUBLE);
                        ps.setString(8, "");
                        if (horaFin != null && !horaFin.isEmpty()) {
                            ps.setString(2, horaFin);
                            ps.setString(5, "Fin día ");
                            try {
                                ps.setInt(7, Integer.parseInt(kmsFin));
                            } catch (Exception e) {
                                ps.setNull(7, Types.INTEGER);
                            }
                        } else {
                            ps.setString(2, horaInicio);
                            ps.setString(5, "Inicio día ");
                            try {
                                ps.setInt(7, Integer.parseInt(kmsInicio));
                            } catch (Exception e) {
                                ps.setNull(7, Types.INTEGER);
                            }
                        }
                        break;
                    }
                    case "descanso": {
                        ps.setNull(6, Types.DOUBLE);
                        ps.setNull(7, Types.INTEGER);
                        ps.setString(8, "");
                        String comentario = "";
                        switch (tipo) {
                            case "0":
                                comentario += "Obligatorio";
                                break;
                            case "1":
                                comentario += "Comida";
                                break;
                            case "2":
                                comentario += "Taller";
                                break;
                            default:
                                comentario += "Otro";
                                break;
                        }
                        if ((horaFin != null && !horaFin.isEmpty())) {
                            ps.setString(2, horaFin);
                            ps.setString(5, "Fin descanso: " + comentario);
                        } else {
                            ps.setString(2, horaInicio);
                            ps.setString(5, "Inicio descanso: " + comentario);
                        }
                        break;
                    }
                    case "repostaje": {
                        ps.setString(2, horaInicio);
                        ps.setString(5, "Repostaje");
                        ps.setString(8, "");
                        try {
                            ps.setDouble(6, Double.parseDouble(litrosRepostaje));
                        } catch (IncompatibleClassChangeError e) {
                            ps.setNull(6, Types.DOUBLE);
                        }
                        try {
                            ps.setDouble(7, Integer.parseInt(kmsInicio));
                        } catch (IncompatibleClassChangeError e) {
                            ps.setNull(7, Types.INTEGER);
                        }
                        break;
                    }
                }
                Logger.getLogger(GrabarDatosCBDriverXT.class.getName()).log(Level.INFO, "Intentando grabar:\n"
                        + ps);
                ps.execute();
                Logger.getLogger(GrabarDatosCBDriverXT.class.getName()).log(Level.INFO, "El estado ha sido grabado\n"
                        + "Conductor " + codigoConductor + " - Cabeza " + matriculaCabeza + " - Hoja de Ruta " + codigoHojaRuta);
            } catch (SQLException ex) {
                Logger.getLogger(GrabarDatosCBDriverXT.class.getName()).log(Level.SEVERE, null, ex);
            }
        } catch (SQLException ex) {
            Logger.getLogger(GrabarDatosCBDriverXT.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
}
