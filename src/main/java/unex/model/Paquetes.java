package unex.model;

import com.datastax.oss.driver.api.mapper.annotations.Entity;
import com.datastax.oss.driver.api.mapper.annotations.PartitionKey;

import java.math.BigDecimal;
import java.util.UUID;

@Entity
public class Paquetes {
    @PartitionKey
    private UUID paqueteId;
    private String nombre;
    private UUID destinoId;
    private int duracion;
    private BigDecimal precio;

    // Atributos adicionales para establecer relaciones
    private Destinos destino;

    // Constructor, getters y setters

    public Paquetes() {
        // Constructor vacío requerido por el mapeador de objetos
    }

    public Paquetes(UUID paqueteId, String nombre, UUID destinoId, int duracion, BigDecimal precio) {
        this.paqueteId = paqueteId;
        this.nombre = nombre;
        this.destinoId = destinoId;
        this.duracion = duracion;
        this.precio = precio;
    }

    // Resto del código...

    public Destinos getDestino() {
        return destino;
    }

    public void setDestino(Destinos destino) {
        this.destino = destino;
    }
}
