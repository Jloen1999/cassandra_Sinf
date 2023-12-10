package unex.model;

import com.datastax.oss.driver.api.mapper.annotations.Entity;
import com.datastax.oss.driver.api.mapper.annotations.PartitionKey;

import java.util.UUID;

@Entity
public class Destinos {
    @PartitionKey
    private UUID DestinosId;
    private String nombre;
    private String pais;
    private String descripcion;
    private String clima;

    // Constructor, getters y setters

    public Destinos() {
        // Constructor vacío requerido por el mapeador de objetos
    }

    public Destinos(UUID DestinosId, String nombre, String pais, String descripcion, String clima) {
        this.DestinosId = DestinosId;
        this.nombre = nombre;
        this.pais = pais;
        this.descripcion = descripcion;
        this.clima = clima;
    }

    public UUID getDestinosId() {
        return DestinosId;
    }

    public void setDestinosId(UUID DestinosId) {
        this.DestinosId = DestinosId;
    }

    public String getNombre() {
        return nombre;
    }

    public void setNombre(String nombre) {
        this.nombre = nombre;
    }

    public String getPais() {
        return pais;
    }

    public void setPais(String pais) {
        this.pais = pais;
    }

    public String getDescripcion() {
        return descripcion;
    }

    public void setDescripcion(String descripcion) {
        this.descripcion = descripcion;
    }

    public String getClima() {
        return clima;
    }

    public void setClima(String clima) {
        this.clima = clima;
    }

    @Override
    public String toString() {
        return "Destinos{" +
                "DestinosId=" + DestinosId +
                ", nombre='" + nombre + '\'' +
                ", pais='" + pais + '\'' +
                ", descripcion='" + descripcion + '\'' +
                ", clima='" + clima + '\'' +
                '}';
    }
}
