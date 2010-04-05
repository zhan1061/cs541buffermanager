import java.awt.BorderLayout;
import java.awt.Button;
import java.awt.Color;
import java.awt.FlowLayout;
import java.awt.Font;
import java.awt.Frame;
import java.awt.Panel;
import java.awt.event.*;
import javax.swing.*;

public class ButtonTest extends JFrame {
	JButton _btnHello = null;	
	JPanel _pnlButtonPanel = null;
	JTextArea _txtStatus = null;
	JScrollPane _scpTxtStatus = null;
	int _timesClicked = 0;
	
	public ButtonTest(String name, int width, int height){
		super(name);
		setSize(width, height);
				
		// Add window listener.
		addWindowListener(new MyWindowAdapter());
		
		// Add panel.
		_pnlButtonPanel = new JPanel();
		_pnlButtonPanel.setLayout(new FlowLayout(FlowLayout.LEFT));
		getContentPane().add(_pnlButtonPanel, BorderLayout.NORTH);
		
		// Add status text area.
		_txtStatus = new JTextArea("");
		
		// Add scroll pane(s).
		_scpTxtStatus = new JScrollPane(_txtStatus);
		getContentPane().add(_scpTxtStatus);
				
		// Add buttons.
		_btnHello = new JButton("Hello");
		_pnlButtonPanel.add(_btnHello);
		
		_btnHello.addActionListener(new HelloActionListener());		
	}
	
	class HelloActionListener implements ActionListener{
		@Override
		public void actionPerformed(ActionEvent event) {
			_txtStatus.append(event.getActionCommand());
			System.out.println(event.getActionCommand());
		}	
	}
	
	class MyWindowAdapter extends WindowAdapter{
		public void windowClosing(WindowEvent event){
			System.exit(0);
		}
	}
}
