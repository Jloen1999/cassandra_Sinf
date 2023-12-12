package unex;

import java.io.IOException;

import static org.fusesource.jansi.Ansi.Color.*;
import static org.fusesource.jansi.Ansi.ansi;

/**
 * @author Jose Luis Obiang Ela Nanguang
 * @version 1.0
 */
public class Main {
    public static void main(String[] args) throws IOException {
        System.out.println(ansi().fg(CYAN).a("   ___   _   ___ ___   _   _  _ ___  ___    _   \n" +
                "  / __| /_\\ / __/ __| /_\\ | \\| |   \\| _ \\  /_\\  \n" +
                " | (__ / _ \\\\__ \\__ \\/ _ \\| .` | |) |   / / _ \\ \n" +
                "  \\___/_/ \\_\\___/___/_/ \\_\\_|\\_|___/|_|_\\/_/ \\_\\\n" +
                "                                                ").reset());
        System.out.println(ansi().fg(YELLOW).a("╔═══════════════════════════════════════════════╗\n" +
                ansi().fg(YELLOW).a("║ ").reset()+"\uD83D\uDC68\uD83C\uDFFE\u200D\uD83C\uDF93 Alumno: Jose Luis Obiang Ela Nanguang     "+ansi().fg(YELLOW).a(" ║\n").reset() +
                ansi().fg(YELLOW).a("║ ").reset()+"\uD83D\uDCD3 Profesor: Francisco Chávez de la O         "+ansi().fg(YELLOW).a("║\n").reset() +
                ansi().fg(YELLOW).a("║ ").reset()+"\uD83D\uDCD8 Asignatura: Sistema de Información (Sinf)  "+ansi().fg(YELLOW).a("║\n").reset() +
                ansi().fg(YELLOW).a("║ ").reset()+"\uD83D\uDCC5 Fecha de entrega: 10-12-2024 23:55         "+ansi().fg(YELLOW).a("║\n").reset() +
                ansi().fg(YELLOW).a("╚═══════════════════════════════════════════════╝\n").reset()).reset());
        // Obtener el nombre de usuario de la máquina
        String user = System.getProperty("user.name");

        // Mostrar ASCII Text banner de la aplicación

        System.out.println("¡Hola, " + user + "! \uD83C\uDF1F Bienvenido a nuestra emocionante aplicación de gestión de agencia de viajes \uD83C\uDF0D\uD83D\uDCBC.\nAquí podrás descubrir destinos increíbles \uD83D\uDDFA\uFE0F, explorar paquetes turísticos fascinantes y gestionar \nlas reservas de tus clientes de manera eficiente \uD83D\uDCC5\uD83D\uDCBB." +
                "¡Disfruta de la experiencia y haz que cada viaje\nsea inolvidable! \uD83D\uDE80✨¿Listo para comenzar tu próxima aventura? ¡Aquí estamos para ayudarte! \uD83C\uDF08\uD83C\uDF1F");
        System.out.println(ansi().fg(YELLOW).a("======================================================================================================").reset());;
        System.out.println(ansi().fg(BLUE).a("    ___                         _              __        _    ___         _              \n" +
                "   /   | ____ ____  ____  _____(_)___ _   ____/ /__     | |  / (_)___ _  (_)__  _____    \n" +
                "  / /| |/ __ `/ _ \\/ __ \\/ ___/ / __ `/  / __  / _ \\    | | / / / __ `/ / / _ \\/ ___/    \n" +
                " / ___ / /_/ /  __/ / / / /__/ / /_/ /  / /_/ /  __/    | |/ / / /_/ / / /  __(__  )     \n" +
                "/_/  |_\\__, /\\___/_/ /_/\\___/_/\\__,_/   \\__,_/\\___/     |___/_/\\__,_/_/ /\\___/____/      \n" +
                "      /____/                                                       /___/                 ").reset());
        System.out.println(ansi().fg(YELLOW).a("======================================================================================================").reset());;
        // Crear el controlador de Cassandra
        CassandraConnector connector = new CassandraConnector();

        // Realizar consultas
        connector.menuPrincipal();

        // Cerrar la sesión
        connector.getSession().close();

    }
}