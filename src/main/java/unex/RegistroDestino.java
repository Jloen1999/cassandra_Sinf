package unex;

import java.util.UUID;

/**
 * @author Jose Luis Obiang Ela Nanguang
 * @version 1.0
 */
public // Clase auxiliar para almacenar los registros
class RegistroDestino {
    private UUID destinoId;
    private String nombreDestino;
    private int totalReservado;

    public RegistroDestino(UUID destinoId, String nombreDestino, int totalReservado) {
        this.destinoId = destinoId;
        this.nombreDestino = nombreDestino;
        this.totalReservado = totalReservado;
    }

    public UUID getDestinoId() {
        return destinoId;
    }

    public String getNombreDestino() {
        return nombreDestino;
    }

    public int getTotalReservado() {
        return totalReservado;
    }
}
